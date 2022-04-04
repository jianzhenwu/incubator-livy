/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.livy.launcher.runner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.client.common.LauncherConf;
import org.apache.livy.client.http.BatchRestClient;
import org.apache.livy.client.http.exception.ServiceUnavailableException;
import org.apache.livy.client.http.param.BatchOptions;
import org.apache.livy.client.http.response.BatchSessionViewResponse;
import org.apache.livy.client.http.response.SessionLogResponse;
import org.apache.livy.launcher.LivyOption;
import org.apache.livy.launcher.exception.LauncherExitCode;
import org.apache.livy.launcher.exception.LivyLauncherException;
import org.apache.livy.launcher.util.CipherUtils;
import org.apache.livy.launcher.util.LauncherUtils;
import org.apache.livy.launcher.util.UriUtil;
import org.apache.livy.sessions.SessionState;

import static org.apache.livy.launcher.LivyLauncherConfiguration.HADOOP_USER_RPCPASSWORD;

public class SparkSubmitRunner {

  private static final Logger logger =
      LoggerFactory.getLogger(SparkSubmitRunner.class);

  private static final byte STDOUT_READ_END = 0b001;
  private static final byte STDERR_READ_END = 0b010;
  private static final byte YARN_DIAGNOSTICS_READ_END = 0b100;
  private static final byte APP_LOG_READ_END = 0b111;

  private static final String AES_SECRET = "livy.aes.secret";

  private final BatchRestClient restClient;
  private final LivyOption livyOptions;
  private final Configuration hadoopConf = new Configuration();
  private final FileSystem fileSystem;
  private final UserGroupInformation userGroupInformation;
  private boolean running = true;
  private Path sessionDir;
  private final boolean waitAppCompletion;
  private int logStage;
  /**
   * 0b001: Read to the end of stdout log.
   * 0b010: Read to the end of stderr log.
   * 0b100: Read to the end of yarnDiagnostics log.
   */
  private int logReadEnd;
  private int fromStdout = 0, fromStderr = 0, fromDiagnostics = 0;

  public SparkSubmitRunner(LivyOption livyOption) {

    try {
      this.livyOptions = livyOption;

      waitAppCompletion = Boolean.parseBoolean(livyOptions.getSparkProperties()
          .getOrDefault("spark.yarn.submit.waitAppCompletion", "true").trim());

      String livyUrl = livyOption.getLivyUrl();

      if (StringUtils.isBlank(livyUrl)) {
        logger.error("Option --livy-url can not be empty.");
        throw new LivyLauncherException(LauncherExitCode.optionError);
      }

      boolean s3aEnabled = Boolean.parseBoolean(livyOption.getLauncherConf()
          .getProperty(LauncherConf.Entry.FS_S3A_ENABLED.key()));
      if (s3aEnabled) {
        loadS3aConf();
      } else {
        loadHadoopConf();
      }
      this.fileSystem = FileSystem.newInstance(this.hadoopConf);

      userGroupInformation =
          UserGroupInformation.createRemoteUser(livyOption.getUsername());
      Credentials credentials = new Credentials();
      credentials.addSecretKey(new Text(HADOOP_USER_RPCPASSWORD),
          livyOption.getPassword().getBytes(StandardCharsets.UTF_8));
      userGroupInformation.addCredentials(credentials);

      URI livyUri = UriUtil
          .appendAuthority(URI.create(livyUrl), livyOption.getUsername(),
              livyOption.getPassword());

      Properties properties = livyOption.getLauncherConf();
      this.restClient = new BatchRestClient(livyUri, properties);

      Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    } catch (Exception e) {
      throw new LivyLauncherException(LauncherExitCode.others, e.getMessage(),
          e.getCause());
    }

  }

  /**
   * Submit application.PythonInterpreterSpec.scala
   * Exit when appId, SparkUiUrl and DriverLogUrl can be obtained.
   */
  public int run() {

    BatchOptions batchOptions;
    try {
      batchOptions = uploadResources(livyOptions.createBatchOptions());
    } catch (IOException e) {
      throw new LivyLauncherException(LauncherExitCode.others, e.getMessage(),
          e.getCause());
    }
    BatchSessionViewResponse sessionView =
        this.restClient.submitBatchJob(batchOptions);
    int sessionId = sessionView.getId();
    logger.info("Application has been submitted. The session id is {}.", sessionId);

    int exitCode = 0;

    while (running || logReadEnd != APP_LOG_READ_END) {
      try {
        sessionView = this.restClient.getBatchSessionView();
        exitCode =
            LauncherUtils.batchSessionExitCode(sessionView.getState()).getCode();
        String appId = sessionView.getAppId();
        if ("running".equalsIgnoreCase(sessionView.getState())
            && !waitAppCompletion) {
          break;
        }
        // Print job submit log before reporting application state.
        if (logStage == 0) {
          printSessionLog();
        }
        // Print Tracking URL at the end of log.
        if (logStage == 1 && StringUtils.isNotBlank(appId)) {
          String trackingUrl = livyOptions.getLivyConfByKey(
              LauncherConf.Entry.SESSION_TRACKING_URL.key());
          String appTrack = StringUtils.isNotBlank(trackingUrl) ?
              "Tracking URL: " + String.format(trackingUrl, appId) :
              "Application ID: " + appId;
          logger.info(appTrack);
          logStage = 2;
        }
        // Report application state.
        if (logStage == 2 && StringUtils.isNotBlank(appId) &&
            StringUtils.isNotBlank(sessionView.getState())) {
          logger.info("Application report for {} (state: {})", appId,
              sessionView.getState().toUpperCase(Locale.ENGLISH));
          Thread.sleep(2800);
        }

        running = SessionState.apply(sessionView.getState()).isActive();
        if (!running) {
          // Make sure all logs are printed after the application is finished.
          printSessionLog();
        }
        Thread.sleep(200);
      } catch (ConnectException | ServiceUnavailableException ce) {
        logger.warn("Please wait, the session {} is recovering.", sessionId);
      } catch (Exception e) {
        throw new LivyLauncherException(LauncherExitCode.others, e.getMessage(),
            e.getCause());
      }
    }
    cleanSessionDir();
    return exitCode;
  }

  private void loadHadoopConf() {
    String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
    if (StringUtils.isBlank(hadoopConfDir)) {
      logger.error("System environment <HADOOP_CONF_DIR> can not be empty.");
      throw new LivyLauncherException(LauncherExitCode.others);
    }
    this.hadoopConf.addResource(new Path(hadoopConfDir, "core-site.xml"));
    this.hadoopConf.addResource(new Path(hadoopConfDir, "hdfs-site.xml"));

    String fsHdfsImpl = "fs.hdfs.impl";
    if (StringUtils.isBlank(this.hadoopConf.get(fsHdfsImpl))) {
      this.hadoopConf.set(fsHdfsImpl, "org.apache.hadoop.hdfs.DistributedFileSystem");
    }
  }

  private void loadS3aConf() {
    String s3aConfFile;
    String livyHome = System.getenv("LIVY_LAUNCHER_HOME");
    if (StringUtils.isBlank(livyHome)) {
      logger.error("System environment LIVY_LAUNCHER_HOME not set");
      throw new LivyLauncherException(LauncherExitCode.others);
    } else {
      s3aConfFile = livyHome + "/conf/livy-s3a.conf";
    }

    if (!new File(s3aConfFile).exists()) {
      logger.error("livy-s3a config file {} does not exist", s3aConfFile);
      throw new LivyLauncherException(LauncherExitCode.others,
          "Cannot find livy-s3a config file");
    }

    logger.info("Load livy-s3a conf file {}.", s3aConfFile);
    Properties props = new Properties();
    try (FileInputStream fileInputStream = new FileInputStream(s3aConfFile)) {
      props.load(fileInputStream);
      props.forEach((key, value) -> {
        if (key.toString().equals("fs.s3a.secret.key")) {
          String secretKey = CipherUtils.decrypt(AES_SECRET, value.toString());
          this.hadoopConf.set(key.toString(), secretKey);
        } else {
          this.hadoopConf.set(key.toString(), value.toString());
        }
      });
    } catch (IOException e) {
      logger.error("Fail to load livy-s3a config file {}.", s3aConfFile);
      throw new LivyLauncherException(LauncherExitCode.others, e.getMessage());
    }
  }

  private BatchOptions uploadResources(BatchOptions batchOptions)
      throws IOException {
    String file = batchOptions.getFile();
    if (StringUtils.isNotBlank(file)) {
      batchOptions.setFile(this.copyFileToRemote(file));
    }
    batchOptions.setJars(uploadResources(batchOptions.getJars()));
    batchOptions.setFiles(uploadResources(batchOptions.getFiles()));
    batchOptions.setPyFiles(uploadResources(batchOptions.getPyFiles()));
    batchOptions.setArchives(uploadResources(batchOptions.getArchives()));
    return batchOptions;
  }

  private List<String> uploadResources(List<String> localFiles)
      throws IOException {
    List<String> remoteFiles = new ArrayList<>();
    if (!localFiles.isEmpty()) {
      for (String localFile : localFiles) {
        remoteFiles.add(this.copyFileToRemote(localFile));
      }
    }
    return remoteFiles;
  }

  /**
   * file: copyFileToRemote,
   * hdfs or s3 file: do nothing
   *
   * @param srcFile srcFile
   * @throws IOException e
   */
  private String copyFileToRemote(String srcFile) throws IOException {

    String srcFilePath = StringUtils.substringBeforeLast(srcFile, "#");
    String unpackDir = StringUtils.substringAfterLast(srcFile, "#");

    URI uri = URI.create(srcFilePath);
    Path srcPath = new Path(uri);
    if (uri.getScheme() == null) {
      srcPath = FileSystem.getLocal(this.hadoopConf).makeQualified(srcPath);
    }

    String FILE = "file";
    if (FILE.equals(srcPath.toUri().getScheme())) {
      Path sessionDir = this.getSessionDir();
      Path destPath = new Path(sessionDir, srcPath.getName());
      String destScheme = destPath.toUri().getScheme();
      if (destScheme == null || FILE.equals(destScheme)) {
        logger.error("Unsupported stagingDir {}. Please update the value of "
                + "livy.session.staging-dir in livy-launcher.conf.%n",
            sessionDir.getParent());
        throw new LivyLauncherException(LauncherExitCode.others);
      }
      FileSystem destFs = destPath.getFileSystem(this.hadoopConf);
      Path finalSrcPath = srcPath;
      userGroupInformation.doAs((PrivilegedAction<Boolean>) () -> {
        try {
          logger.info("Uploading resource {} -> {}.", finalSrcPath, destPath);
          FileUtil.copy(
              finalSrcPath.getFileSystem(SparkSubmitRunner.this.hadoopConf),
              finalSrcPath, destFs, destPath, false,
              SparkSubmitRunner.this.hadoopConf);
          FsPermission permission =
              FsPermission.createImmutable((short) Integer.parseInt("644", 8));
          destFs.setPermission(destPath, new FsPermission(permission));
          return true;
        } catch (IOException e) {
          logger.error("Failed to upload resource {}.", finalSrcPath);
          throw new LivyLauncherException(LauncherExitCode.others,
              e.getMessage(), e.getCause());
        }
      });
      return destPath +
          (StringUtils.isNotBlank(unpackDir) ? "#" + unpackDir : "");
    } else {
      return srcFile;
    }
  }

  private Path getSessionDir() {
    if (sessionDir == null) {
      // "livy.session.staging-dir"
      Path stagingDir = new Path(livyOptions
          .getLivyConfByKey(LauncherConf.Entry.SESSION_STAGING_DIR.key(),
              new Path(fileSystem.getHomeDirectory(), ".livy-sessions")
                  .toString()));

      sessionDir = new Path(stagingDir, UUID.randomUUID().toString());
      userGroupInformation.doAs((PrivilegedAction<Boolean>) () -> {
        try {
          fileSystem.mkdirs(sessionDir);
          fileSystem.setPermission(sessionDir, new FsPermission("700"));
          return true;
        } catch (IOException e) {
          throw new LivyLauncherException(LauncherExitCode.others,
              e.getMessage(), e.getCause());
        }
      });
    }
    return sessionDir;
  }

  private void printSessionLog(String logType, int from)
      throws ConnectException {
    int size = 100;
    SessionLogResponse sessionLogResponse =
        this.restClient.getSessionLog(from, size, logType);
    List<String> logs = sessionLogResponse.getLog();
    boolean isReadEnd = (from + size) >= sessionLogResponse.getTotal();

    switch (logType) {
      case "stdout":
        fromStdout += logs.size();
        logReadEnd = isReadEnd ? logReadEnd | STDOUT_READ_END
            : logReadEnd & ~STDOUT_READ_END;
        break;
      case "stderr":
        fromStderr += logs.size();
        logReadEnd = isReadEnd ? logReadEnd | STDERR_READ_END
            : logReadEnd & ~STDERR_READ_END;
        break;
      case "yarnDiagnostics":
        fromDiagnostics += logs.size();
        logReadEnd = isReadEnd ? logReadEnd | YARN_DIAGNOSTICS_READ_END
            : logReadEnd & ~YARN_DIAGNOSTICS_READ_END;
        break;
      default:
    }
    for (String log : sessionLogResponse.getLog()) {
      logStage = log.trim().startsWith("tracking URL:") ? 1 : logStage;
      System.err.println(log);
    }
  }

  private void printSessionLog() throws ConnectException {
    printSessionLog("stdout", fromStdout);
    printSessionLog("stderr", fromStderr);
    printSessionLog("yarnDiagnostics", fromDiagnostics);
  }

  /**
   * Clean session directory if application is not in running state.
   */
  public void cleanSessionDir() {
    if (!this.running && this.sessionDir != null) {
      userGroupInformation.doAs((PrivilegedAction<Boolean>) () -> {
        try {
          fileSystem.delete(this.sessionDir, true);
          logger.debug("Clean session directory.");
        } catch (IOException io) {
          logger.error("Fail to clean session directory {}.", this.sessionDir,
              io);
        }
        return true;
      });
    }
  }

  public void close() {
    restClient.stop(this.waitAppCompletion);
    logger.debug("Close session connection.");
    try {
      this.fileSystem.close();
    } catch (IOException io) {
      logger.error("Fail to close FileSystem.");
    }
  }
}
