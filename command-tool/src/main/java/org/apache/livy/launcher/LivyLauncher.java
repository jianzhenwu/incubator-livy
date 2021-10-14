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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.launcher.exception.LivyLauncherException;
import org.apache.livy.launcher.runner.PysparkRunner;
import org.apache.livy.launcher.runner.SparkShellRunner;
import org.apache.livy.launcher.runner.SparkSqlRunner;
import org.apache.livy.launcher.runner.SparkSubmitRunner;

public class LivyLauncher {

  private static final Logger logger =
      LoggerFactory.getLogger(LivyLauncher.class);

  public static void main(String[] args) {

    String sparkCommand = args[0];

    List<String> sparkArgs = new ArrayList<>();
    if (args.length > 1) {
      sparkArgs.addAll(Arrays.asList(args).subList(1, args.length));
    } else {
      // Default to print usage
      sparkArgs.add("--help");
    }

    int exitCode = 0;
    try {
      switch (sparkCommand) {
        case "spark-submit":
          SparkSubmitOption sparkSubmitOption =
              new SparkSubmitOption(sparkArgs);
          SparkSubmitRunner sparkSubmitRunner =
              new SparkSubmitRunner(sparkSubmitOption);
          exitCode = sparkSubmitRunner.run();
          break;
        case "spark-sql":
          SparkSqlOption sparkSqlOption = new SparkSqlOption(sparkArgs);
          SparkSqlRunner sparkSqlRunner = new SparkSqlRunner(sparkSqlOption);
          exitCode = sparkSqlRunner.interactive();
          break;
        case "pyspark":
          PySparkOption pySparkOption = new PySparkOption(sparkArgs);
          PysparkRunner pysparkRunner = new PysparkRunner(pySparkOption);
          exitCode = pysparkRunner.interactive();
          break;
        case "spark-shell":
          SparkShellOption sparkShellOption = new SparkShellOption(sparkArgs);
          SparkShellRunner sparkShellRunner =
              new SparkShellRunner(sparkShellOption);
          exitCode = sparkShellRunner.interactive();
          break;
        default:
          logger.error("Unsupported sparkCommand {}", sparkCommand);
          System.exit(1);
      }
    } catch (LivyLauncherException e) {
      logger.info("Fail to start livy session.", e);
      System.exit(e.getExitCode().getCode());
    }
    System.exit(exitCode);
  }
}
