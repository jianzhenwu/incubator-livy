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
package org.apache.livy.launcher;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.ByteUtils;
import org.apache.livy.client.http.param.BatchOptions;
import org.apache.livy.client.http.param.InteractiveOptions;
import org.apache.livy.launcher.exception.LauncherExitCode;
import org.apache.livy.launcher.exception.LivyLauncherException;
import org.apache.livy.launcher.util.LauncherUtils;

public class LivyOption extends LivyOptionParser {

  private static final Logger logger =
      LoggerFactory.getLogger(LivyOption.class);
  private final PrintStream infoStream = new PrintStream(System.out, true);

  private String livyUrl;
  private String username;
  private String password;
  private String proxyUser;
  private List<String> jars = new ArrayList<>();
  private List<String> pyFiles = new ArrayList<>();
  private List<String> files = new ArrayList<>();
  private String driverMemory;
  private Integer driverCores;
  private String executorMemory;
  private Integer executorCores;
  private Integer numExecutors;
  private List<String> archives = new ArrayList<>();
  private String queue;
  private String name;
  private String propertiesFile;
  /**
   * Spark configuration properties
   */
  private final Map<String, String> sparkProperties = new HashMap<>();

  /**
   * Interactive session kind (spark, pyspark, sparkr, or sql)
   */
  private String kind;
  /**
   * Timeout in second to which interactive session be orphaned
   */
  private int heartbeatTimeoutInSecond;

  /**
   * File containing the application to execute
   */
  private String file;
  /**
   * Application Java/Spark main class
   */
  private String className;
  /**
   * User application main args when spark-submit, or other args of pyspark and
   * spark-sql command.
   */
  private List<String> extraArgs;

  /**
   * launcher configuration
   */
  private final Properties launcherConf = new Properties();

  private boolean dynamicAllocationEnabled;

  /**
   * ALl arguments passed by command line tool.
   */
  private final List<String> sparkArgs;

  /**
   * Throw IllegalArgumentException when valued option has no value,
   * throw UnhandledOptionException when option is recognized and unhandled,
   * throw LivyLauncherException when should exit.
   */
  public LivyOption(List<String> sparkArgs, String kind,
      DefaultExtraArgsParser extraArgsParser) {
    super(extraArgsParser);
    this.sparkArgs = sparkArgs;
    this.kind = kind;

    parse(sparkArgs);
    this.loadLauncherConf();
    this.mergeDefaultSparkProperties();
    this.loadEnvironmentArguments();
    this.validateOptions(sparkArgs);
  }

  @Override
  protected boolean handle(String arg, String value) {
    switch (arg) {
      case CLASS:
        this.className = value;
        break;
      case NAME:
        this.name = value;
        break;
      case JARS:
        this.jars.addAll(LauncherUtils.splitByComma(value));
        break;
      case FILES:
        this.files.addAll(LauncherUtils.splitByComma(value));
        break;
      case CONF:
        int idx = value.indexOf("=");
        try {
          String k = value.substring(0, idx);
          String v = value.substring(idx + 1);
          // Allow space parameters.
          this.sparkProperties.putIfAbsent(k.trim(), v);
        } catch (Exception e) {
          throw new IllegalArgumentException(
              String.format("Illegal Argument %s %s", CONF, value));
        }
        break;
      case DRIVER_MEMORY:
        this.driverMemory = value;
        break;
      case EXECUTOR_MEMORY:
        this.executorMemory = value;
        break;
      case PROXY_USER:
        this.proxyUser = value;
        break;
      case DRIVER_CORES:
        this.driverCores = Integer.parseInt(value);
        break;
      case EXECUTOR_CORES:
        this.executorCores = Integer.parseInt(value);
        break;
      case QUEUE:
        this.queue = value;
        break;
      case NUM_EXECUTORS:
        this.numExecutors = Integer.parseInt(value);
        break;
      case ARCHIVES:
        this.archives.addAll(LauncherUtils.splitByComma(value));
        break;
      case PY_FILES:
        this.pyFiles.addAll(LauncherUtils.splitByComma(value));
        break;
      case REPOSITORIES:
        this.sparkProperties.put("spark.jars.repositories", value);
        break;
      case PACKAGES:
        this.sparkProperties.put("spark.jars.packages", value);
        break;
      case EXCLUDE_PACKAGES:
        this.sparkProperties.put("spark.jars.excludes", value);
        break;
      case DRIVER_JAVA_OPTIONS:
        this.sparkProperties.put("spark.driver.extraJavaOptions", value);
        break;
      case DRIVER_LIBRARY_PATH:
        this.sparkProperties.put("spark.driver.extraLibraryPath", value);
        break;
      case DRIVER_CLASS_PATH:
        this.sparkProperties.put("spark.driver.extraClassPath", value);
        break;
      case PROPERTIES_FILE:
        this.propertiesFile = value;
        break;
      case DEPLOY_MODE:
      case MASTER:
        logger.warn("Option {} is set by livy.", arg);
        break;
      case LIVY_URL:
        this.livyUrl = value;
        break;
      case USERNAME:
        this.username = value;
        break;
      case PASSWORD:
        this.password = value;
        break;
      case HELP:
        this.printUsageAndExit(LauncherExitCode.normal);
        break;
      default:
        return false;
    }
    return true;
  }

  private void printUsageAndExit(LauncherExitCode exitCode) {
    String usage = System.getenv("_LIVY_LAUNCHER_USAGE");
    if (StringUtils.isNotBlank(usage)) {
      infoStream.println(usage);
    }

    String launcherUsage = "Options:\n"
        + "  --class CLASS_NAME          Your application's main class (for Java"
        + " / Scala apps).\n"
        + "  --name NAME                 A name of your application.\n"
        + "  --jars JARS                 Comma-separated list of jars to include"
        + " on the driver\n"
        + "                              and executor classpaths.\n"
        + "  --py-files PY_FILES         Comma-separated list of .zip, .egg, or"
        + " .py files to place\n"
        + "                              on the PYTHONPATH for Python apps.\n"
        + "  --files FILES               Comma-separated list of files to be"
        + " placed in the working\n"
        + "                              directory of each executor. File paths"
        + " of these files\n"
        + "                              in executors can be accessed via"
        + " SparkFiles.get(fileName).\n" + "\n"
        + "  --conf, -c PROP=VALUE       Arbitrary Spark configuration property.\n"
        + "  --properties-file FILE      Path to a file from which to load"
        + " extra properties. If not\n"
        + "                              specified, this will look for"
        + " conf/spark-defaults.conf.\n" + "\n"
        + "  --driver-memory MEM         Memory for driver (e.g. 1000M, 2G)"
        + " (Default: 1024M).\n"
        + "  --executor-memory MEM       Memory per executor (e.g. 1000M, 2G)"
        + " (Default: 1G).\n" + "\n"
        + "  --proxy-user NAME           User to impersonate when submitting the"
        + " application.\n"
        + "                              This argument does not work with"
        + " --principal / --keytab.\n" + "\n"
        + "  --help, -h                  Show this help message and exit.\n"
        + "\n" + " Cluster deploy mode only:\n"
        + "  --driver-cores NUM          Number of cores used by the driver,"
        + " only in cluster mode\n"
        + "                              (Default: 1).\n" + "\n"
        + " Spark standalone and YARN only:\n"
        + "  --executor-cores NUM        Number of cores per executor."
        + " (Default: 1 in YARN mode,\n"
        + "                              or all available cores on the worker in"
        + " standalone mode)\n" + "\n" + " YARN-only:\n"
        + "  --queue QUEUE_NAME          The YARN queue to submit to"
        + " (Default: \"default\").\n"
        + "  --num-executors NUM         Number of executors to launch"
        + " (Default: 2).\n"
        + "                              If dynamic allocation is enabled,"
        + " the initial number of\n"
        + "                              executors will be at least NUM.\n"
        + "  --archives ARCHIVES         Comma separated list of archives to be"
        + " extracted into the\n"
        + "                              working directory of each executor.\n";

    infoStream.println(launcherUsage);

    extraArgsParser.printCliUsage();

    throw new LivyLauncherException(exitCode);
  }

  private void loadLauncherConf() {

    String livyConfFile;
    String livyHome = System.getenv("LIVY_LAUNCHER_HOME");
    if (StringUtils.isBlank(livyHome)) {
      logger.debug("System environment LIVY_LAUNCHER_HOME not set");
      return;
    } else {
      livyConfFile = livyHome + "/conf/livy-launcher.conf";
    }

    if (!new File(livyConfFile).exists()) {
      return;
    }

    try (FileInputStream fileInputStream = new FileInputStream(livyConfFile)) {
      this.launcherConf.load(fileInputStream);
    } catch (IOException e) {
      logger.warn("Fail to load livy-launcher config file {}.", livyConfFile);
      return;
    }

    logger.info("Load livy-launcher conf file {}.", livyConfFile);
    Set<Map.Entry<Object, Object>> entries = this.launcherConf.entrySet();
    for (Map.Entry<Object, Object> e : entries) {
      if (LivyLauncherConfiguration.LIVY_URL.equals(e.getKey().toString())) {
        if (StringUtils.isBlank(this.livyUrl)) {
          this.livyUrl = e.getValue().toString();
          logger.info("Set livy-url = {}.", e.getValue());
          break;
        }
      }
    }
  }

  private void mergeDefaultSparkProperties() {

    if (StringUtils.isBlank(this.propertiesFile)) {
      return;
    }

    Properties properties = new Properties();
    try (FileInputStream fileInputStream = new FileInputStream(
        propertiesFile)) {
      properties.load(fileInputStream);
    } catch (IOException io) {
      logger.error("Fail to load propertiesFile {}.", this.propertiesFile);
      throw new LivyLauncherException(LauncherExitCode.others);
    }
    Set<Map.Entry<Object, Object>> entries = properties.entrySet();
    for (Map.Entry<Object, Object> e : entries) {

      // Do not overwrite options which are set by user.
      if (!this.sparkProperties.containsKey(e.getKey().toString())) {
        this.sparkProperties
            .put(e.getKey().toString(), e.getValue().toString());
      }
    }
  }

  private void loadEnvironmentArguments() {

    username = Optional.ofNullable(username).orElse(
        Optional.ofNullable(System.getenv(
            LivyLauncherConfiguration.HADOOP_USER_NAME))
            .orElse(System.getProperty("user.name")));

    try {
      password = Optional.ofNullable(password).orElse(
          Optional.ofNullable(System.getenv(
              LivyLauncherConfiguration.HADOOP_USER_RPCPASSWORD))
              .orElse(LauncherUtils.passwordFromSDI()));
    } catch (IOException io) {
      logger.warn("Fail to load HADOOP_USER_RPCPASSWORD from sdi.", io);
    }

    driverMemory = Optional.ofNullable(driverMemory).orElse(
        Optional.ofNullable(sparkProperties.get("spark.driver.memory"))
            .orElse(System.getenv().get("SPARK_DRIVER_MEMORY")));

    driverCores = Optional.ofNullable(driverCores)
        .orElse(LauncherUtils.firstInteger(sparkProperties.get("spark.driver.cores")));

    executorMemory = Optional.ofNullable(executorMemory).orElse(
        Optional.ofNullable(sparkProperties.get("spark.executor.memory"))
            .orElse(System.getenv().get("SPARK_EXECUTOR_MEMORY")));

    executorCores = Optional.ofNullable(executorCores).orElse(
        LauncherUtils.firstInteger(sparkProperties.get("spark.executor.cores"),
            System.getenv().get("SPARK_EXECUTOR_CORES")));

    name =
        Optional.ofNullable(name).orElse(sparkProperties.get("spark.app.name"));
    jars = Optional.ofNullable(jars)
        .orElse(LauncherUtils.splitByComma(sparkProperties.get("spark.jars")));
    files = Optional.ofNullable(files)
        .orElse(LauncherUtils.splitByComma(sparkProperties.get("spark.files")));
    archives = Optional.ofNullable(archives)
        .orElse(LauncherUtils.splitByComma(sparkProperties.get("spark.archives")));
    pyFiles = Optional.ofNullable(pyFiles)
        .orElse(LauncherUtils.splitByComma(sparkProperties.get("spark.submit.pyFiles")));

    numExecutors = Optional.ofNullable(numExecutors)
        .orElse(LauncherUtils.firstInteger(sparkProperties.get("spark.executor.instances")));
    queue = Optional.ofNullable(queue)
        .orElse(sparkProperties.get("spark.yarn.queue"));

    dynamicAllocationEnabled = "true".equalsIgnoreCase(Optional
        .ofNullable(sparkProperties.get("spark.dynamicAllocation.enabled"))
        .orElse("false"));
  }

  /**
   * Check whether the options are legal.
   * Throw IllegalArgumentException when the option is illegal.
   *
   * @param args Args passed by livy command tool.
   */
  protected void validateOptions(List<String> args) {
    if (args.size() == 0) {
      printUsageAndExit(LauncherExitCode.optionError);
    }
    if (password == null) {
      String message = String.format("Rpc password empty from client side for user: %s, "
          + "please set `HADOOP_USER_RPCPASSWORD` in the system environment, "
          + "or check sdi credentials in %s",
          username, String.format("%s/.sdi/credentials", System.getProperty("user.home")));
      throw new IllegalArgumentException(message);
    }
    if (driverMemory != null
        && ByteUtils.byteStringAsBytes(driverMemory) <= 0) {
      throw new IllegalArgumentException(
          "Driver memory must be a positive number");
    }
    if (executorMemory != null
        && ByteUtils.byteStringAsBytes(executorMemory) <= 0) {
      throw new IllegalArgumentException(
          "Executor memory must be a positive number");
    }
    if (driverCores != null && driverCores <= 0) {
      throw new IllegalArgumentException(
          "Driver cores must be a positive number");
    }
    if (executorCores != null && executorCores <= 0) {
      throw new IllegalArgumentException(
          "Executor cores must be a positive number");
    }
    if (!dynamicAllocationEnabled && numExecutors != null
        && numExecutors <= 0) {
      throw new IllegalArgumentException(
          "Number of executors must be a positive number");
    }
  }

  public String getLivyConfByKey(String key, String defaultValue) {
    return this.launcherConf.getProperty(key, defaultValue);
  }

  public String getLivyConfByKey(String key) {
    return this.launcherConf.getProperty(key);
  }

  public InteractiveOptions createInteractiveOptions() {
    return new InteractiveOptions().setKind(this.kind)
        .setHeartbeatTimeoutInSecond(this.heartbeatTimeoutInSecond)
        .setProxyUser(this.proxyUser).setJars(this.jars)
        .setPyFiles(this.pyFiles).setFiles(this.files)
        .setDriverMemory(this.driverMemory).setDriverCores(this.driverCores)
        .setExecutorMemory(this.executorMemory)
        .setExecutorCores(this.executorCores).setNumExecutors(this.numExecutors)
        .setArchives(this.archives).setQueue(this.queue).setName(this.name)
        .setConf(this.sparkProperties);
  }

  /**
   * Should create an interactive session without resources before upload resources.
   *
   * @return InteractiveOptions
   */
  public InteractiveOptions createInteractiveOptionsWithoutResources() {
    return new InteractiveOptions().setKind(this.kind)
        .setProxyUser(this.proxyUser).setDriverMemory(this.driverMemory)
        .setDriverCores(this.driverCores).setExecutorMemory(this.executorMemory)
        .setExecutorCores(this.executorCores).setNumExecutors(this.numExecutors)
        .setQueue(this.queue).setName(this.name).setConf(this.sparkProperties)
        .setHeartbeatTimeoutInSecond(this.heartbeatTimeoutInSecond);
  }

  public BatchOptions createBatchOptions() {
    return new BatchOptions().setFile(file).setProxyUser(proxyUser)
        .setClassName(className).setArgs(extraArgs).setJars(jars)
        .setPyFiles(pyFiles).setFiles(files).setDriverMemory(driverMemory)
        .setDriverCores(driverCores).setExecutorMemory(executorMemory)
        .setExecutorCores(executorCores).setNumExecutors(numExecutors)
        .setArchives(archives).setQueue(queue).setName(name)
        .setConf(sparkProperties);
  }

  public String getLivyUrl() {
    return livyUrl;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getProxyUser() {
    return proxyUser;
  }

  public List<String> getJars() {
    return jars;
  }

  public List<String> getPyFiles() {
    return pyFiles;
  }

  public List<String> getFiles() {
    return files;
  }

  public String getDriverMemory() {
    return driverMemory;
  }

  public Integer getDriverCores() {
    return driverCores;
  }

  public String getExecutorMemory() {
    return executorMemory;
  }

  public Integer getExecutorCores() {
    return executorCores;
  }

  public Integer getNumExecutors() {
    return numExecutors;
  }

  public List<String> getArchives() {
    return archives;
  }

  public String getQueue() {
    return queue;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getSparkProperties() {
    return sparkProperties;
  }

  @VisibleForTesting
  public String getKind() {
    return kind;
  }

  public int getHeartbeatTimeoutInSecond() {
    return heartbeatTimeoutInSecond;
  }

  public String getFile() {
    return file;
  }

  public LivyOption setFile(String file) {
    this.file = file;
    return this;
  }

  public String getClassName() {
    return className;
  }

  public void setExtraArgs(List<String> extraArgs) {
    this.extraArgs = extraArgs;
  }

  public Properties getLauncherConf() {
    return launcherConf;
  }
}
