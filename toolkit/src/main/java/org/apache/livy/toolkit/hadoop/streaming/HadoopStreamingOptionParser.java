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
package org.apache.livy.toolkit.hadoop.streaming;

import java.io.PrintStream;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import org.apache.livy.toolkit.hadoop.streaming.exception.HadoopStreamingException;
import org.apache.livy.toolkit.hadoop.streaming.exception.LauncherExitCode;

public class HadoopStreamingOptionParser {

  protected final String INPUT = "input";
  protected final String OUTPUT = "output";
  protected final String MAPPER = "mapper";
  protected final String PARTITIONER = "partitioner";
  protected final String REDUCER = "reducer";
  protected final String INPUT_FORMAT = "inputformat";
  protected final String OUTPUT_FORMAT = "outputformat";
  protected final String CMD_ENV = "cmdenv";
  protected final String CONF = "D";

  protected final String INFO = "info";
  protected final String HELP = "help";

  protected Options options;
  private final PrintStream infoStream = new PrintStream(System.out, true);

  protected void setupOptions() {
    Option input = createOption(INPUT,
                                "DFS input file(s) for the Map step",
                                "path", false);
    Option output = createOption(OUTPUT,
                                 "DFS output directory for the Reduce step",
                                 "path", false);
    Option mapper = createOption(MAPPER,
                                 "The streaming command to run",
                                 "cmd", false);
    Option partitioner = createOption(PARTITIONER,
                                      "Optional.",
                                      "spec", false);
    Option reducer = createOption(REDUCER,
                                  "The streaming command to run",
                                  "cmd", false);
    Option inputFormat = createOption(INPUT_FORMAT,
                                      "Optional.",
                                      "spec", false);
    Option outputFormat = createOption(OUTPUT_FORMAT,
                                       "Optional.",
                                       "spec", false);
    Option cmdenv = createOption(CMD_ENV,
                                 "(n=v) Pass env.var to streaming commands.",
                                 "spec", false);
    Option conf = createOption(CONF,
                               "(n=v) Optional. Add or override a JobConf property.",
                               "spec", false);
    Option info = createBoolOption(INFO, "print verbose output");
    Option help = createBoolOption(HELP, "print this help message");

    options = new Options()
        .addOption(input)
        .addOption(output)
        .addOption(mapper)
        .addOption(partitioner)
        .addOption(reducer)
        .addOption(inputFormat)
        .addOption(outputFormat)
        .addOption(cmdenv)
        .addOption(conf)
        .addOption(info)
        .addOption(help);
  }

  private Option createOption(String name, String desc,
      String argName, boolean required) {
    return OptionBuilder
        .withArgName(argName)
        .hasArgs()
        .withDescription(desc)
        .isRequired(required)
        .create(name);
  }

  private Option createBoolOption(String name, String desc){
    return OptionBuilder.withDescription(desc).create(name);
  }

  protected void printUsageAndExit(boolean detailed, LauncherExitCode exitCode) {

    String launcherUsage = "Options:\n"
        + "  -input                     <path> DFS input file(s) for the Map step.\n"
        + "  -output                    <path> DFS output directory for the Reduce step.\n"
        + "  -mapper                    <cmd|JavaClassName> Optional. Command"
        + " to be run as mapper.\n"
        + "  -partitioner               <JavaClassName>  Optional. The partitioner class.\n"
        + "  -reducer                   <cmd|JavaClassName> Optional. Command"
        + " to be run as reducer.\n"
        + "  -inputformat               <TextInputFormat(default)|SequenceFileAsTextInputFormat"
        + "|JavaClassName> Optional. The input format class.\n"
        + "  -outputformat              <TextOutputFormat(default)|JavaClassName>"
        + " Optional. The output format class.\n"
        + "  -cmdenv                    <n>=<v> Optional. Pass env.var to"
        + " streaming commands.\n"
        + "  -D                         <n>=<v> Optional. set custom job conf.\n"
        + "  -info                      Optional. Print detailed usage.\n"
        + "  -help                      Optional. Print help message.\n";

    infoStream.println(launcherUsage);

    if (!detailed) {
      System.out.println();
      System.out.println("For more details about these options:");
      System.out.println("Usage: -info");
      throw new HadoopStreamingException(exitCode);
    }

    String launcherDetailedUsage = "Usage tips:\n"
        + "In -input: globbing on <path> is supported and can have multiple -input\n"
        + "Default Map input format:\n"
        + "  a line is a record in UTF-8 the key part ends at first TAB, "
        + "the rest of the line is the value\n"
        + "To pass a Custom input format:\n"
        + "  -inputformat package.MyInputFormat\n"
        + "Similarly, to pass a custom output format:\n"
        + "  -outputformat package.MyOutputFormat\n"
        + "To set an environment variable in a streaming command:\n"
        + "  -cmdenv EXAMPLE_DIR /home/example/dictionaries/\n"
        + "Example: $HSTREAMING -mapper /usr/local/bin/perl5 filter.pl"
        + "  -input /logs/0604*/*\" [...]"
        + "  -output /tmp/output/\n"
        + "  Ships a script, invokes the non-shipped perl interpreter. Shipped files go to\n"
        + "  the working directory so filter.pl is found by perl. \n"
        + "  Input files are all the daily logs for days in month 2006-04\n";

    infoStream.println(launcherDetailedUsage);

    throw new HadoopStreamingException(exitCode);
  }
}
