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

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import jline.console.ConsoleReader;
import jline.console.history.FileHistory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import org.apache.livy.client.http.HttpClient;
import org.apache.livy.client.http.response.StatementOutput;
import org.apache.livy.client.http.response.StatementResponse;
import org.apache.livy.launcher.LivyOption;
import org.apache.livy.launcher.exception.LauncherExitCode;
import org.apache.livy.launcher.exception.LivyLauncherException;
import org.apache.livy.launcher.util.LauncherUtils;
import org.apache.livy.launcher.util.UriUtil;

public abstract class AbstractInteractiveRunner {

  protected HttpClient restClient;
  protected ConsoleReader consoleReader;

  protected boolean exit;
  protected int exitCode = 0;
  protected StringBuilder builder;
  protected String prompt;

  private final LivyOption livyOption;
  private FileHistory fileHistory;

  public AbstractInteractiveRunner(LivyOption livyOption) {
    try {
      this.livyOption = livyOption;
      String livyUrl = livyOption.getLivyUrl();
      if (StringUtils.isBlank(livyUrl)) {
        logger().error("Option livy-url can not be empty.");
        throw new LivyLauncherException(LauncherExitCode.optionError);
      }

      URI livyUri = UriUtil
          .appendAuthority(URI.create(livyUrl), livyOption.getUsername(),
              livyOption.getPassword());

      // It should create session first then upload resource from launcher host.
      this.restClient = new HttpClient(livyUri, livyOption.getLauncherConf(),
          livyOption.createInteractiveOptionsWithoutResources());

      Runtime.getRuntime().addShutdownHook(new Thread(this::close));

      this.restClient.waitUntilSessionStarted();
      this.restClient
          .addOrUploadResources(livyOption.createInteractiveOptions());

      this.initConsoleReader();

    } catch (Exception e) {
      throw new LivyLauncherException(LauncherExitCode.others, e.getMessage(), e.getCause());
    }
  }

  private void initConsoleReader() throws IOException {
    this.consoleReader =
        new ConsoleReader("livy-launcher", System.in, new PrintStream(System.out, true),
            null, "utf-8");
    this.consoleReader.setHistoryEnabled(true);

    fileHistory = new FileHistory(this.createHistoryFile());
    this.consoleReader.setHistory(fileHistory);
    this.addComplete(this.consoleReader);
  }

  private File createHistoryFile() {
    String historyFileDir = System.getProperty("user.home");
    if (!new File(historyFileDir).exists()) {
      historyFileDir = String.format("/tmp/%s", System.getProperty("user.name"));
      new File(historyFileDir).mkdirs();
    }

    String historyFile =
        String.format("%s/.livy-%s", historyFileDir, this.livyOption.getKind());
    logger().debug("ConsoleReader create history file {}", historyFile);
    return new File(historyFile);
  }

  /**
   * Add auto complete function for user input.
   */
  public void addComplete(ConsoleReader consoleReader) {
  }

  /**
   * The runner may be interrupted by user.
   * Any exceptions need to be caught and output to the terminal.
   */
  public int interactive() {
    try {
      builder = new StringBuilder();
      prompt = promptStart();
      while (!exit) {
        String line = this.consoleReader.readLine(prompt);
        if (line == null) {
          if (builder.length() == 0) {
            exit = true;
            break;
          } else {
            builder.setLength(0);
            prompt = promptStart();
            this.consoleReader.println();
            continue;
          }
        }

        handleLine(line);
      }
    } catch (IOException e) {
      throw new LivyLauncherException(LauncherExitCode.others, e.getMessage(),
          e.getCause());
    }
    return this.exitCode;
  }

  /**
   * Handle line that read from consoleReader.
   */
  public abstract void handleLine(String line);

  public void handleStatementResponse(StatementResponse statementResponse) {

    StatementOutput output = statementResponse.getOutputData();
    this.exitCode = LauncherUtils
        .statementExitCode(statementResponse.getState(), output.getStatus())
        .getCode();

    List<List<String>> res = output.getData();
    outputStatementResult(res);
  }

  /**
   * Return start prompt.
   */
  public abstract String promptStart();

  /**
   * Return later prompt.
   */
  public abstract String promptContinue();

  public void close() {
    restClient.stop(true);
    try {
      if (fileHistory != null) {
        fileHistory.flush();
      }
    } catch (IOException e) {
      logger().error("Fail to close fileHistory of consoleReader, ", e);
    }
    try {
      if (consoleReader != null) {
        consoleReader.flush();
        consoleReader.shutdown();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    logger().info("Close session connection.");
  }

  public void outputStatementResult(List<List<String>> res) {
    for (List<String> rowRaw : res) {
      List<String> row = rowRaw.stream().map(e -> e != null ? e : "NULL")
          .collect(Collectors.toList());
      try {
        this.consoleReader.println(String.join("\t", row));
      } catch (IOException e) {
        logger().error("Fail to output statement result.");
      }
    }
  }

  /**
   * The subclass of logger can intuitively see which runner the log comes from.
   * @return Logger
   */
  protected abstract Logger logger();
}
