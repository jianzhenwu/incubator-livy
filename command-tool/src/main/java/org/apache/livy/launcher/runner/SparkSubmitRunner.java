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

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.util.*;

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

import org.apache.livy.client.http.BatchRestClient;
import org.apache.livy.client.http.param.BatchOptions;
import org.apache.livy.client.http.response.BatchSessionViewResponse;
import org.apache.livy.client.http.response.SessionStateResponse;
import org.apache.livy.launcher.LivyOption;
import org.apache.livy.launcher.exception.LauncherExitCode;
import org.apache.livy.launcher.exception.LivyLauncherException;
import org.apache.livy.launcher.util.LauncherUtils;
import org.apache.livy.launcher.util.UriUtil;
import org.apache.livy.sessions.SessionState;

import static org.apache.livy.launcher.LivyLauncherConfiguration.HADOOP_USER_RPCPASSWORD;
import static org.apache.livy.launcher.LivyLauncherConfiguration.SESSION_STAGING_DIR;

public class SparkSubmitRunner {

  private static final Logger logger =
      LoggerFactory.getLogger(SparkSubmitRunner.class);

  private final BatchRestClient restClient;
  private final LivyOption livyOptions;
  private final Configuration hadoopConf;
  private final FileSystem fileSystem;
  private final UserGroupInformation userGroupInformation;
  private boolean running = true;
  private Path sessionDir;
  private final boolean waitAppCompletion;
  private String appId;
  private String sparkUiUrl;

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

      String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
      if (StringUtils.isBlank(hadoopConfDir)) {
        logger.error("System environment <HADOOP_CONF_DIR> can not be empty.");
        throw new LivyLauncherException(LauncherExitCode.others);
      }
      this.hadoopConf = new Configuration();
      this.hadoopConf.addResource(new Path(hadoopConfDir, "core-site.xml"));
      this.hadoopConf.addResource(new Path(hadoopConfDir, "hdfs-site.xml"));

      String fsHdfsImpl = "fs.hdfs.impl";
      if (StringUtils.isBlank(this.hadoopConf.get(fsHdfsImpl))) {
        this.hadoopConf
            .set(fsHdfsImpl, "org.apache.hadoop.hdfs.DistributedFileSystem");
      }
      this.fileSystem = FileSystem.get(this.hadoopConf);

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
    logger.info("Application has been submitted. The session id is {}.",
        sessionView.getId());

    int exitCode =
        LauncherUtils.batchSessionExitCode(sessionView.getState()).getCode();

    // The appStarted method should be called first. It will log SparkUiUrl.
    if (this.appStarted(sessionView) && !waitAppCompletion) {
      return exitCode;
    }

    while (running) {

      try {
        Thread.sleep(1000);

        sessionView = this.restClient.getBatchSessionView();
        exitCode =
            LauncherUtils.batchSessionExitCode(sessionView.getState()).getCode();
        if (appStarted(sessionView) && !waitAppCompletion) {
          break;
        }

        // session state
        SessionStateResponse sessionStateResponse =
            this.restClient.getSessionState();

        if (StringUtils.isNotBlank(appId) && StringUtils
            .isNotBlank(sessionStateResponse.getState())) {
          logger.info("Application report for {} (state: {})", this.appId,
              sessionStateResponse.getState().toUpperCase(Locale.ENGLISH));
        }
        running =
            SessionState.apply(sessionStateResponse.getState()).isActive();
      } catch (Exception e) {
        throw new LivyLauncherException(LauncherExitCode.others, e.getMessage(),
            e.getCause());
      }
    }
    cleanSessionDir();
    return exitCode;
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
   * hdfs file: do nothing
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
          FileUtil.copy(
              finalSrcPath.getFileSystem(SparkSubmitRunner.this.hadoopConf),
              finalSrcPath, destFs, destPath, false,
              SparkSubmitRunner.this.hadoopConf);
          FsPermission permission =
              FsPermission.createImmutable((short) Integer.parseInt("644", 8));
          destFs.setPermission(destPath, new FsPermission(permission));
          return true;
        } catch (IOException e) {
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
          .getLivyConfByKey(SESSION_STAGING_DIR,
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

  private boolean appStarted(BatchSessionViewResponse sessionView) {
    // Do not log SparkUiUrl multi time.
    if (StringUtils.isNotBlank(this.sparkUiUrl)) {
      return true;
    }

    this.appId = sessionView.getAppId();

    if (StringUtils.isNotBlank(sessionView.getAppId()) && sessionView
        .hasAppInfoValue()) {
      this.sparkUiUrl = sessionView.getAppInfo().getSparkUiUrl();
      String driverLogUrl = sessionView.getAppInfo().getDriverLogUrl();

      logger.info("ApplicationId: {}\nSparkUiUrl: {}\nDriverLogUrl: {}",
          sessionView.getAppId(), this.sparkUiUrl, driverLogUrl);
      return true;
    }
    return false;
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
