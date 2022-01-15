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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.toolkit.hadoop.streaming.exception.LauncherExitCode;

public class HadoopStreamingOption extends HadoopStreamingOptionParser {

  private static final Logger logger =
      LoggerFactory.getLogger(HadoopStreamingOption.class);

  private String[] input;
  private String output;
  private String mapper;
  private String partitioner;
  private String reducer;
  private String inputFormat;
  private String outputFormat;
  private final Map<String, String> cmdEnv = new HashMap<>();
  private final Map<String, String> conf = new HashMap<>();

  public HadoopStreamingOption(String[] args) {
    this.setupOptions();
    this.parse(args);
    this.validateOptions(args);
  }

  private void parse(String[] args) {
    CommandLine cmdLine = null;
    try {
      cmdLine = new BasicParser().parse(options, args);
    } catch (ParseException e) {
      logger.error(e.getMessage());
      exit(e.getMessage());
    }

    if (cmdLine != null) {
      List<String> remainingArgs = cmdLine.getArgList();
      if (remainingArgs != null && remainingArgs.size() > 0) {
        exit("Found " + remainingArgs.size() + " unsupported arguments on the " +
            "command line " + remainingArgs);
      }
      if (cmdLine.hasOption(INFO)) {
        printUsageAndExit(true, LauncherExitCode.normal);
      }
      if (cmdLine.hasOption(HELP)) {
        printUsageAndExit(false, LauncherExitCode.normal);
      }
      input = cmdLine.getOptionValues(INPUT);
      output = cmdLine.getOptionValue(OUTPUT);
      mapper = cmdLine.getOptionValue(MAPPER);
      partitioner = cmdLine.getOptionValue(PARTITIONER);
      reducer = cmdLine.getOptionValue(REDUCER);
      inputFormat = cmdLine.getOptionValue(INPUT_FORMAT);
      outputFormat = cmdLine.getOptionValue(OUTPUT_FORMAT);

      Properties envOption = cmdLine.getOptionProperties(CMD_ENV);
      envOption.keySet().forEach(e -> {
        int idx = e.toString().indexOf("=");
        String k = e.toString().substring(0, idx);
        String v = e.toString().substring(idx + 1);
        cmdEnv.put(k.trim(), v);
      });
      Properties confOption = cmdLine.getOptionProperties(CONF);
      confOption.keySet().forEach(c -> {
        int idx = c.toString().indexOf("=");
        String k = c.toString().substring(0, idx);
        String v = c.toString().substring(idx + 1);
        conf.put(k.trim(), v);
      });
    } else {
      exit("Required positive arguments");
    }
  }

  private void validateOptions(String[] args) {
    if (args.length < 1) {
      printUsageAndExit(false, LauncherExitCode.optionError);
    }
    if (input == null || input.length == 0) {
      exit("Required argument: -input <path>");
    }
    if (output == null) {
      exit("Required argument: -output <path>");
    }
  }

  private void exit(String message) {
    System.err.println(message);
    System.err.println("Try -help for more information");
    throw new IllegalArgumentException(message);
  }

  public String[] getInput() {
    return input;
  }

  public String getOutput() {
    return output;
  }

  public String getMapper() {
    return mapper;
  }

  public String getPartitioner() {
    return partitioner;
  }

  public String getReducer() {
    return reducer;
  }

  public String getInputFormat() {
    return inputFormat;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public Map<String, String> getCmdEnv() {
    return cmdEnv;
  }

  public Map<String, String> getConf() {
    return conf;
  }
}
