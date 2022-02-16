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

import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;

import org.apache.livy.launcher.exception.LauncherExitCode;
import org.apache.livy.launcher.exception.LivyLauncherException;

public class SparkSqlOption extends LivyOption {

  public SparkSqlOption(List<String> args) {
    super(args, "sql", new SparkSqlExtraArgsParser());

    Properties hiveConf = getHiveConf();
    if (hiveConf != null) {
      Set<Object> keySet = hiveConf.keySet();
      for (Object k : keySet) {
        String key = k.toString();
        if (!key.startsWith("spark.hadoop.")) {
          key = "spark.hadoop." + key;
        }
        this.getSparkProperties().put(key, hiveConf.getProperty(k.toString()));
      }
    }
  }

  public Properties getDefineProperties() {
    return this.getCommandLine().getOptionProperties("define");
  }

  public String getDatabase() {
    return this.getCommandLine().getOptionValue("database");
  }

  public String getQuery() {
    return this.getCommandLine().getOptionValue('e');
  }

  public String getSqlFile() {
    return this.getCommandLine().getOptionValue('f');
  }

  public Properties getHiveConf() {
    return this.getCommandLine().getOptionProperties("hiveconf");
  }

  public Properties getHiveVar() {
    return this.getCommandLine().getOptionProperties("hivevar");
  }

  public String[] getInitFiles() {
    return this.getCommandLine().getOptionValues('i');
  }

  private CommandLine getCommandLine() {
    return extraArgsParser.getCommandLine();
  }

  private static class SparkSqlExtraArgsParser
      extends DefaultExtraArgsParser {

    @Override
    protected Options extraOptions() {
      Options options = new Options();
      // -d,--define <key=value>
      options.addOption(
          OptionBuilder.hasArgs(2).withArgName("key=value").withValueSeparator()
              .withLongOpt("define").withDescription(
              "Variable substitution to apply to Hive "
                  + "commands. e.g. -d A=B or --define A=B").create('d'));

      options.addOption(OptionBuilder.hasArg().withLongOpt("database")
          .withArgName("databasename")
          .withDescription("Specify the database to use").create());

      // -e 'quoted-query-string'
      options.addOption(
          OptionBuilder.hasArg().withArgName("quoted-query-string")
              .withDescription("SQL from command line").create('e'));

      // -f <query-file>
      options.addOption(OptionBuilder.hasArg().withArgName("filename")
          .withDescription("SQL from files").create('f'));

      options.addOption(OptionBuilder.hasArg(false).withLongOpt("help")
          .withDescription("Print help information").create('H'));

      options.addOption(OptionBuilder.hasArgs(2).withArgName("property=value")
          .withValueSeparator().withLongOpt("hiveconf")
          .withDescription("Use value for given property").create());

      options.addOption(
          OptionBuilder.hasArgs(2).withArgName("key=value").withValueSeparator()
              .withLongOpt("hivevar").withDescription(
              "Variable substitution to apply to Hive "
                  + "commands. e.g. --hivevar A=B").create());

      // -i <filename>
      options.addOption(OptionBuilder.hasArg().withArgName("filename")
          .withDescription("Initialization SQL file").create('i'));
      return options;
    }
  }

  @Override
  protected void validateOptions(List<String> args) {
    super.validateOptions(args);
    if (StringUtils.isNotBlank(this.getQuery()) && StringUtils
        .isNotBlank(this.getSqlFile())) {
      throw new LivyLauncherException(LauncherExitCode.optionError,
          "-e and -f options cannot take effect at the same time");
    }
  }
}
