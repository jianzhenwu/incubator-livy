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
import java.net.ConnectException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.history.FileHistory;
import org.slf4j.Logger;

import org.apache.livy.client.common.LauncherConf;
import org.apache.livy.client.common.StatementState;
import org.apache.livy.client.http.HttpClient;
import org.apache.livy.client.http.param.InteractiveOptions;
import org.apache.livy.client.http.response.CancelStatementResponse;
import org.apache.livy.client.http.response.StatementOutput;
import org.apache.livy.client.http.response.StatementResponse;
import org.apache.livy.launcher.Interval;
import org.apache.livy.launcher.LivyOption;
import org.apache.livy.launcher.RetryInterval;
import org.apache.livy.launcher.RetryTask;
import org.apache.livy.launcher.Signaling;
import org.apache.livy.launcher.exception.LauncherExitCode;
import org.apache.livy.launcher.exception.LivyLauncherException;
import org.apache.livy.launcher.util.LauncherUtils;
import org.apache.livy.sessions.SessionState;

public abstract class AbstractInteractiveRunner extends BaseCommandLineRunner {
  private static final String PROCESS_FORMAT = "%-23s %-60s %s";
  private static final int PROGRESS_BAR_CHARS = 60;

  protected HttpClient restClient;
  protected ConsoleReader consoleReader;

  protected boolean exit;
  protected StringBuilder builder;
  protected String prompt;

  private final LauncherConf launcherConf;
  private FileHistory fileHistory;

  public AbstractInteractiveRunner(LivyOption livyOption) {
    super(livyOption);
    try {
      Properties properties = livyOption.getLauncherConf();
      this.launcherConf = new LauncherConf(properties);

      InteractiveOptions interactiveOptions;
      // First upload resource to remote filesystem
      try {
        interactiveOptions =
            (InteractiveOptions) uploadResources(livyOption.createInteractiveOptions());
      } catch (IOException e) {
        throw new LivyLauncherException(LauncherExitCode.others, e.getMessage(),
            e.getCause());
      }
      // It should create session include required resource.
      this.restClient = new HttpClient(livyUri, properties, interactiveOptions);
      this.restClient.waitUntilSessionStarted();
      this.isActive = SessionState.apply(this.restClient.getSessionState().getState()).isActive();

      this.initConsoleReader();

    } catch (Exception e) {
      throw new LivyLauncherException(LauncherExitCode.others, e.getMessage(), e.getCause());
    }
  }

  private void initConsoleReader() throws IOException {
    this.consoleReader =
        new ConsoleReader("livy-launcher", System.in, new PrintStream(System.out, true),
            null, "utf-8");
    this.consoleReader.setHandleUserInterrupt(true);

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
  @Override
  public int run() {
    try {
      builder = new StringBuilder();
      prompt = promptStart();
      while (!exit) {
        String line = null;
        try {
          line = this.consoleReader.readLine(prompt);
        } catch (UserInterruptException e) {
          // 1. ctr + c is same with ctr + d.
          // 2. Cancel current line.
          // 3. Exit if current line is empty.
        }
        if (line == null) {
          if (builder.length() == 0) {
            exit = true;
            this.isActive = false;
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

  public StatementResponse runStatement(String code) {
    long startTime = System.currentTimeMillis();
    long timeout = launcherConf.getTimeAsMs(LauncherConf.Entry.STATEMENT_TIMEOUT);
    long offset = launcherConf.getTimeAsMs(LauncherConf.Entry.STATEMENT_POLLING_INTERVAL_OFFSET);
    long step = launcherConf.getTimeAsMs(LauncherConf.Entry.STATEMENT_POLLING_INTERVAL_STEP);
    long max = launcherConf.getTimeAsMs(LauncherConf.Entry.STATEMENT_POLLING_INTERVAL_MAX);

    Interval interval = new RetryInterval(offset, step, max);

    int finalStatementId =
        new RetryTask<Integer>(restClient, startTime, timeout, interval) {
          @Override
          public Integer task() throws ConnectException {
            StatementResponse submitRes = restClient.submitStatement(code);
            return submitRes.getId();
          }
        }.run();

    // register a signal handler to terminate the current statement in session.
    Signaling.cancelOnInterrupt(new RetryTask<CancelStatementResponse>(
        restClient, startTime, timeout, interval) {
      @Override
      public CancelStatementResponse task() throws ConnectException {
        return restClient.cancelStatement(finalStatementId);
      }
    });

    return new RetryTask<StatementResponse>(restClient, startTime, timeout, interval) {
      @Override
      public StatementResponse task() throws ConnectException {
        StatementResponse runRes = restClient.statementResult(finalStatementId);
        String state = runRes.getState();
        printProgress(runRes.getProgress(), state);
        if (StatementState.isCancel(state) || StatementState.isAvailable(state)) {
          return runRes;
        }
        return null;
      }
    }.run();
  }

  private void printProgress(double percent, String state) {
    String progress = String.format(
        PROCESS_FORMAT,
        new Timestamp(System.currentTimeMillis()),
        getInPlaceProgressBar(percent),
        (int) (percent * 100) + "%");
    try {
      if (StatementState.isActive(state) || StatementState.isAvailable(state)) {
        this.consoleReader.print("\r");
        this.consoleReader.print(progress);
      }
      if (StatementState.isCancel(state) || StatementState.isAvailable(state)) {
        this.consoleReader.println();
      }
      this.consoleReader.flush();
    } catch (IOException e) {
      logger().error("Failed to print statement execute progress.");
    }
  }

  private String getInPlaceProgressBar(double percent) {
    StringWriter bar = new StringWriter();
    bar.append("[");
    int remainingChars = PROGRESS_BAR_CHARS - 4;
    int completed = (int) (remainingChars * percent);
    int pending = remainingChars - completed;
    for (int i = 0; i < completed; i++) {
      bar.append("=");
    }
    bar.append(">");
    for (int i = 0; i < pending; i++) {
      bar.append(" ");
    }
    bar.append("]");
    return bar.toString();
  }

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

  @Override
  public void close() {
    if (restClient != null) {
      restClient.stop(true);
    }
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
    super.close();
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
