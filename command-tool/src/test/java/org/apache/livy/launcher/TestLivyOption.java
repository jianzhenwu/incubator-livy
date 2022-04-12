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

import java.util.Arrays;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.apache.livy.launcher.exception.LivyLauncherException;

public class TestLivyOption {

  @Test
  public void testSparkSubmitOption() {
    String[] args =
        { "--class", "Main", "--conf", "spark.sql.shuffle.partitions=300",
            "--driver-cores", "1", "--executor-memory", "1G", "--files",
            "file1, file2", "--jars", "jar1, jar2", "--name", "testParser",
            "--proxy-user", "proxy-user", "--py-files", "file1, file2",
            "--repositories", "myRepositories",
            "--packages", "package1",
            "--conf", "spark.jars.packages=package2",
            "--exclude-packages", "ex_package",
            "--driver-java-options", "opt1",
            "--driver-library-path", "/path/driver/library",
            "--driver-class-path", "classpath",
            "--archives", "archive1, archive2", "--executor-cores", "1",
            "--num-executors", "2", "--queue", "dev", "--livy-url", "livy-url",
            "--username", "username", "--password", "password", "a.jar" };
    SparkSubmitOption mockSparkSubmitOption =
        new SparkSubmitOption(Arrays.asList(args));

    assertEquals(mockSparkSubmitOption.getClassName(), "Main");
    assertEquals(mockSparkSubmitOption.getSparkProperties()
        .get("spark.sql.shuffle.partitions"), "300");
    assertEquals(mockSparkSubmitOption.getDriverCores().intValue(), 1);
    assertEquals(mockSparkSubmitOption.getExecutorMemory(), "1G");
    assertEquals(mockSparkSubmitOption.getFiles().get(0), "file1");
    assertEquals(mockSparkSubmitOption.getFiles().get(1), "file2");
    assertEquals(mockSparkSubmitOption.getJars().get(0), "jar1");
    assertEquals(mockSparkSubmitOption.getJars().get(1), "jar2");
    assertEquals(mockSparkSubmitOption.getName(), "testParser");
    assertEquals(mockSparkSubmitOption.getProxyUser(), "proxy-user");
    assertEquals(mockSparkSubmitOption.getPyFiles().get(0), "file1");
    assertEquals(mockSparkSubmitOption.getPyFiles().get(1), "file2");
    Map<String, String> sparkConf = mockSparkSubmitOption.getSparkProperties();
    assertEquals(sparkConf.get("spark.jars.repositories"), "myRepositories");
    assertEquals(sparkConf.get("spark.jars.packages"), "package1");
    assertEquals(sparkConf.get("spark.jars.excludes"), "ex_package");
    assertEquals(sparkConf.get("spark.driver.extraJavaOptions"), "opt1");
    assertEquals(sparkConf.get("spark.driver.extraLibraryPath"), "/path/driver/library");
    assertEquals(sparkConf.get("spark.driver.extraClassPath"), "classpath");
    assertEquals(mockSparkSubmitOption.getArchives().get(0), "archive1");
    assertEquals(mockSparkSubmitOption.getArchives().get(1), "archive2");
    assertEquals(mockSparkSubmitOption.getExecutorCores().intValue(), 1);
    assertEquals(mockSparkSubmitOption.getNumExecutors().intValue(), 2);
    assertEquals(mockSparkSubmitOption.getQueue(), "dev");
    assertEquals(mockSparkSubmitOption.getLivyUrl(), "livy-url");
    assertEquals(mockSparkSubmitOption.getUsername(), "username");
    assertEquals(mockSparkSubmitOption.getPassword(), "password");
    assertEquals(mockSparkSubmitOption.getFile(), "a.jar");
  }

  @Test
  public void testSparkSqlOption() {
    String[] args =
        { "--conf", "spark.sql.shuffle.partitions=300", "--driver-cores", "1",
            "--executor-memory", "1G", "--name", "testParser", "--proxy-user",
            "proxy-user", "--executor-cores", "1", "--num-executors", "2",
            "--queue", "dev", "--livy-url", "livy-url", "--username",
            "username", "--password", "password", "-d", "def1=a", "--database",
            "database", "-e", "select * from table;", "--hiveconf", "conf1=a",
            "--hivevar", "var1=a", };
    SparkSqlOption mockSparkSqlOption = new SparkSqlOption(Arrays.asList(args));

    assertEquals(mockSparkSqlOption.getKind(), "sql");
    assertEquals(mockSparkSqlOption.getSparkProperties()
        .get("spark.sql.shuffle.partitions"), "300");
    assertEquals(mockSparkSqlOption.getDriverCores().intValue(), 1);
    assertEquals(mockSparkSqlOption.getExecutorMemory(), "1G");
    assertEquals(mockSparkSqlOption.getName(), "testParser");
    assertEquals(mockSparkSqlOption.getProxyUser(), "proxy-user");
    assertEquals(mockSparkSqlOption.getExecutorCores().intValue(), 1);
    assertEquals(mockSparkSqlOption.getNumExecutors().intValue(), 2);
    assertEquals(mockSparkSqlOption.getQueue(), "dev");
    assertEquals(mockSparkSqlOption.getLivyUrl(), "livy-url");
    assertEquals(mockSparkSqlOption.getUsername(), "username");
    assertEquals(mockSparkSqlOption.getPassword(), "password");

    assertEquals(mockSparkSqlOption.getDatabase(), "database");
    assertEquals(mockSparkSqlOption.getDefineProperties().getProperty("def1"),
        "a");
    assertEquals(mockSparkSqlOption.getQuery(), "select * from table;");
    assertEquals(mockSparkSqlOption.getHiveConf().getProperty("conf1"), "a");
    assertEquals(mockSparkSqlOption.getHiveVar().getProperty("var1"), "a");
  }

  /**
   * Throw exception when Unrecognized option: --database
   */
  @Test(expected = LivyLauncherException.class)
  public void testSparkShellOption() {
    String[] args =
        { "--driver-cores", "1", "--executor-memory", "1G", "--name",
            "testParser", "--queue", "dev", "--livy-url", "livy-url",
            "--username", "username", "--password", "password", "--database",
            "database" };
    SparkShellOption mockParser = new SparkShellOption(Arrays.asList(args));

    assertEquals(mockParser.getKind(), "spark");
    assertEquals(mockParser.getDriverCores().intValue(), 1);
    assertEquals(mockParser.getExecutorMemory(), "1G");
    assertEquals(mockParser.getName(), "testParser");
    assertEquals(mockParser.getQueue(), "dev");
    assertEquals(mockParser.getLivyUrl(), "livy-url");
    assertEquals(mockParser.getUsername(), "username");
    assertEquals(mockParser.getPassword(), "password");
  }

  @Test
  public void testPySparkOption() {
    String[] args =
        { "--driver-cores", "1", "--executor-memory", "1G", "--name",
            "testParser", "--queue", "dev", "--livy-url", "livy-url",
            "--username", "username", "--password", "password"};
    PySparkOption mockParser = new PySparkOption(Arrays.asList(args));

    assertEquals(mockParser.getKind(), "pyspark");
    assertEquals(mockParser.getDriverCores().intValue(), 1);
    assertEquals(mockParser.getExecutorMemory(), "1G");
    assertEquals(mockParser.getName(), "testParser");
    assertEquals(mockParser.getQueue(), "dev");
    assertEquals(mockParser.getLivyUrl(), "livy-url");
    assertEquals(mockParser.getUsername(), "username");
    assertEquals(mockParser.getPassword(), "password");
  }
}
