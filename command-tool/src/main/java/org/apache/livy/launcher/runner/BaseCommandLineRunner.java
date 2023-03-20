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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.apache.livy.client.http.param.BaseOptions;
import org.apache.livy.launcher.LivyOption;
import org.apache.livy.launcher.exception.LauncherExitCode;
import org.apache.livy.launcher.exception.LivyLauncherException;
import org.apache.livy.launcher.util.CipherUtils;
import org.apache.livy.launcher.util.UriUtil;

import static org.apache.livy.launcher.LivyLauncherConfiguration.HADOOP_USER_RPCPASSWORD;

public abstract class BaseCommandLineRunner {

  public static Logger logger = LoggerFactory.getLogger(BaseCommandLineRunner.class);

  protected LivyOption livyOption;
  private final Configuration hadoopConf = new Configuration();
  private final FileSystem fileSystem;
  private final UserGroupInformation userGroupInformation;

  protected URI livyUri;
  protected Path sessionDir;
  protected boolean isActive;
  protected int exitCode = 0;

  public BaseCommandLineRunner(LivyOption livyOption) {
    try {
      this.livyOption = livyOption;
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

      this.userGroupInformation =
          UserGroupInformation.createRemoteUser(livyOption.getUsername());
      Credentials credentials = new Credentials();
      credentials.addSecretKey(new Text(HADOOP_USER_RPCPASSWORD),
          livyOption.getPassword().getBytes(StandardCharsets.UTF_8));
      this.userGroupInformation.addCredentials(credentials);

      this.livyUri = UriUtil
          .appendAuthority(URI.create(livyUrl), livyOption.getUsername(),
              livyOption.getPassword());

      Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    } catch (Exception e) {
      throw new LivyLauncherException(LauncherExitCode.others, e.getMessage(),
          e.getCause());
    }
  }

  protected int run() {
    return exitCode;
  }

  protected void close() {
    try {
      this.fileSystem.close();
    } catch (IOException io) {
      logger.error("Fail to close FileSystem.");
    }
  }

  protected void loadHadoopConf() {
    String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
    if (StringUtils.isBlank(hadoopConfDir)) {
      logger.error("System environment <HADOOP_CONF_DIR> can not be empty.");
      throw new LivyLauncherException(LauncherExitCode.others);
    }
    hadoopConf.addResource(new Path(hadoopConfDir, "core-site.xml"));
    hadoopConf.addResource(new Path(hadoopConfDir, "hdfs-site.xml"));

    String fsHdfsImpl = "fs.hdfs.impl";
    if (StringUtils.isBlank(hadoopConf.get(fsHdfsImpl))) {
      hadoopConf.set(fsHdfsImpl, "org.apache.hadoop.hdfs.DistributedFileSystem");
    }
  }

  protected void loadS3aConf() {
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
      Map<String, String> sparkProperties = livyOption.getSparkProperties();
      props.load(fileInputStream);
      props.forEach((key, value) -> {
        if (key.toString().equals("fs.s3a.secret.key")) {
          String secretKey = CipherUtils.decrypt(value.toString());
          hadoopConf.set(key.toString(), secretKey);
          sparkProperties.put("spark.hadoop." + key, secretKey);
        } else {
          hadoopConf.set(key.toString(), value.toString());
          if (!key.toString().equals("fs.defaultFS")) {
            sparkProperties.put("spark.hadoop." + key, value.toString());
          }
        }
      });
      sparkProperties.put("spark.livy.s3a.enabled", "true");
    } catch (IOException e) {
      logger.error("Fail to load livy-s3a config file {}.", s3aConfFile);
      throw new LivyLauncherException(LauncherExitCode.others, e.getMessage());
    }
  }

  protected BaseOptions uploadResources(BaseOptions options)
      throws IOException {
    options.setJars(uploadResources(options.getJars()));
    options.setFiles(uploadResources(options.getFiles()));
    options.setPyFiles(uploadResources(options.getPyFiles()));
    options.setArchives(uploadResources(options.getArchives()));
    return options;
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
  protected String copyFileToRemote(String srcFile) throws IOException {

    String srcFilePath = StringUtils.substringBeforeLast(srcFile, "#");
    String unpackDir = StringUtils.substringAfterLast(srcFile, "#");

    URI uri = URI.create(srcFilePath);
    Path srcPath = new Path(uri);
    if (uri.getScheme() == null) {
      srcPath = FileSystem.getLocal(hadoopConf).makeQualified(srcPath);
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
      FileSystem destFs = destPath.getFileSystem(hadoopConf);
      Path finalSrcPath = srcPath;
      userGroupInformation.doAs((PrivilegedAction<Boolean>) () -> {
        try {
          logger.info("Uploading resource {} -> {}.", finalSrcPath, destPath);
          FileUtil.copy(
              finalSrcPath.getFileSystem(hadoopConf),
              finalSrcPath, destFs, destPath, false,
              hadoopConf);
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
    if (this.sessionDir == null) {
      // "livy.session.staging-dir"
      Path stagingDir = new Path(livyOption
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

  /**
   * Clean session directory if application is not in running state.
   */
  protected void cleanSessionDir() {
    if (!this.isActive && this.sessionDir != null) {
      userGroupInformation.doAs((PrivilegedAction<Boolean>) () -> {
        try {
          fileSystem.delete(sessionDir, true);
          logger.debug("Clean session directory.");
        } catch (IOException io) {
          logger.error("Fail to clean session directory {}.", sessionDir,
              io);
        }
        return true;
      });
    }
  }
}
