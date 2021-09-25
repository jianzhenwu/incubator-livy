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

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.launcher.exception.LivyLauncherException;

class DefaultExtraArgsParser {

  private static final Logger logger =
      LoggerFactory.getLogger(DefaultExtraArgsParser.class);

  protected final Options options;

  private CommandLine commandLine;

  public DefaultExtraArgsParser() {
    this.options = this.extraOptions();
  }

  protected Options extraOptions() {
    return new Options();
  }

  public void parse(String[] argv) {
    try {
      commandLine = new DefaultParser().parse(options, argv);

      if (commandLine.hasOption("help")) {
        printCliUsage();
        throw new LivyLauncherException(0);
      }
    } catch (ParseException e) {
      logger.error("Fail to parse extra args", e);
      printCliUsage();
      throw new LivyLauncherException(1);
    }
  }

  public void printCliUsage() {
    if (!this.options.getOptions().isEmpty()) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.setSyntaxPrefix("");
      formatter.printHelp("CLI options:", options);
    }
  }

  public CommandLine getCommandLine() {
    return commandLine;
  }
}
