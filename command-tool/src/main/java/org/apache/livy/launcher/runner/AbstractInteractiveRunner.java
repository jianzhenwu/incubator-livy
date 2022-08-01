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
import java.net.URI;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.history.FileHistory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import org.apache.livy.client.common.LauncherConf;
import org.apache.livy.client.common.StatementState;
import org.apache.livy.client.http.HttpClient;
import org.apache.livy.client.http.exception.AuthServerException;
import org.apache.livy.client.http.exception.ServiceUnavailableException;
import org.apache.livy.client.http.exception.TimeoutException;
import org.apache.livy.client.http.response.StatementOutput;
import org.apache.livy.client.http.response.StatementResponse;
import org.apache.livy.launcher.LivyOption;
import org.apache.livy.launcher.exception.LauncherExitCode;
import org.apache.livy.launcher.exception.LivyLauncherException;
import org.apache.livy.launcher.util.LauncherUtils;
import org.apache.livy.launcher.util.UriUtil;

public abstract class AbstractInteractiveRunner {
  private static final String PROCESS_FORMAT = "%-23s %-60s %s";
  private static final int PROGRESS_BAR_CHARS = 60;

  protected HttpClient restClient;
  protected ConsoleReader consoleReader;

  protected boolean exit;
  protected int exitCode = 0;
  protected StringBuilder builder;
  protected String prompt;

  private final LivyOption livyOption;
  private final LauncherConf launcherConf;
  private FileHistory fileHistory;

  public AbstractInteractiveRunner(LivyOption livyOption) {
    try {
      this.livyOption = livyOption;
      this.launcherConf = new LauncherConf(livyOption.getLauncherConf());
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
  public int interactive() {
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
        new RetryTask<Integer>(startTime, timeout, interval) {
          @Override
          public Integer task() throws ConnectException {
            StatementResponse submitRes = restClient.submitStatement(code);
            return submitRes.getId();
          }
        }.run();

    return new RetryTask<StatementResponse>(startTime, timeout, interval) {
      @Override
      public StatementResponse task() throws ConnectException {
        StatementResponse runRes = restClient.statementResult(finalStatementId);
        boolean isAvailable = StatementState.Available.toString().equals(runRes.getState());
        printProgress(runRes.getProgress(), isAvailable);
        if (isAvailable) {
          return runRes;
        }
        return null;
      }
    }.run();
  }

  private void printProgress(double percent, boolean isAvailable) {
    String progress = String.format(
        PROCESS_FORMAT,
        new Timestamp(System.currentTimeMillis()),
        getInPlaceProgressBar(percent),
        (int) (percent * 100) + "%");
    try {
      this.consoleReader.print("\r");
      this.consoleReader.print(progress);
      if (isAvailable) {
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

  static class RetryInterval implements Interval {

    private final long offset;
    private final long step;
    private final long max;

    public RetryInterval(long offset, long step, long max) {
      this.offset = offset;
      this.step = step;
      this.max = max;
    }

    @Override
    public long interval(int count) {
      double t = Math.log(count + 1) * step + offset;
      return Double.valueOf(Math.min(t, max)).longValue();
    }
  }

  interface Interval {
    long interval(int count);
  }

  abstract class RetryTask<T> {

    private final long startTime;
    private final long timeout;
    private final Interval interval;


    public RetryTask(long startTime, long timeout, Interval interval) {
      this.startTime = startTime;
      this.timeout = timeout;
      this.interval = interval;
    }

    /**
     * User define task.
     */
    public abstract T task() throws ConnectException;

    public T run() {

      int count = 0;
      while (true) {
        try {
          T t = task();
          if (t != null) {
            return t;
          }
        } catch (ConnectException | ServiceUnavailableException | AuthServerException e) {
          logger().warn("Please wait, the session {} is recovering.", restClient.getSessionId());
        }

        long currentTime = System.currentTimeMillis();
        if (timeout > 0 && currentTime - startTime > timeout) {
          throw new TimeoutException(
              "Timeout with session " + restClient.getSessionId());
        }

        count += 1;
        try {
          Thread.sleep(this.interval.interval(count));
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
