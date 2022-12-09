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

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.client.common.LauncherConf;
import org.apache.livy.client.http.BatchRestClient;
import org.apache.livy.client.http.exception.AuthServerException;
import org.apache.livy.client.http.exception.ServiceUnavailableException;
import org.apache.livy.client.http.param.BaseOptions;
import org.apache.livy.client.http.param.BatchOptions;
import org.apache.livy.client.http.response.BatchSessionViewResponse;
import org.apache.livy.client.http.response.SessionLogResponse;
import org.apache.livy.launcher.LivyOption;
import org.apache.livy.launcher.exception.LauncherExitCode;
import org.apache.livy.launcher.exception.LivyLauncherException;
import org.apache.livy.launcher.util.LauncherUtils;
import org.apache.livy.sessions.SessionState;

public class SparkSubmitRunner extends BaseCommandLineRunner {

  private static final Logger logger =
      LoggerFactory.getLogger(SparkSubmitRunner.class);

  private static final byte STDOUT_READ_END = 0b001;
  private static final byte STDERR_READ_END = 0b010;
  private static final byte YARN_DIAGNOSTICS_READ_END = 0b100;
  private static final byte APP_LOG_READ_END = 0b111;

  private final BatchRestClient restClient;
  private final boolean waitAppCompletion;
  private int logStage;
  /**
   * 0b001: Read to the end of stdout log.
   * 0b010: Read to the end of stderr log.
   * 0b100: Read to the end of yarnDiagnostics log.
   */
  private int logReadEnd;
  private int fromStdout = 0, fromStderr = 0, fromDiagnostics = 0;

  public SparkSubmitRunner(LivyOption livyOption) {
    super(livyOption);
    try {
      this.waitAppCompletion = Boolean.parseBoolean(livyOption.getSparkProperties()
          .getOrDefault("spark.yarn.submit.waitAppCompletion", "true").trim());

      Properties properties = livyOption.getLauncherConf();
      this.restClient = new BatchRestClient(livyUri, properties);
    } catch (Exception e) {
      throw new LivyLauncherException(LauncherExitCode.others, e.getMessage(),
          e.getCause());
    }
  }

  /**
   * Submit application.PythonInterpreterSpec.scala
   * Exit when appId, SparkUiUrl and DriverLogUrl can be obtained.
   */
  @Override
  public int run() {

    BatchOptions batchOptions;
    try {
      batchOptions = (BatchOptions) uploadResources(livyOption.createBatchOptions());
    } catch (IOException e) {
      throw new LivyLauncherException(LauncherExitCode.others, e.getMessage(),
          e.getCause());
    }
    BatchSessionViewResponse sessionView =
        this.restClient.submitBatchJob(batchOptions);
    this.isActive = SessionState.apply(sessionView.getState()).isActive();
    int sessionId = sessionView.getId();
    logger.info("Application has been submitted. The session id is {}.", sessionId);

    while (isActive || logReadEnd != APP_LOG_READ_END) {
      try {
        sessionView = this.restClient.getBatchSessionView();
        exitCode =
            LauncherUtils.batchSessionExitCode(sessionView.getState()).getCode();
        String appId = sessionView.getAppId();
        if ("running".equalsIgnoreCase(sessionView.getState())
            && !waitAppCompletion) {
          break;
        }
        // Print job submit log before reporting application state.
        if (logStage == 0) {
          printSessionLog();
        }
        // Print Tracking URL at the end of log.
        if (logStage == 1 && StringUtils.isNotBlank(appId)) {
          String trackingUrl = livyOption.getLivyConfByKey(
              LauncherConf.Entry.SESSION_TRACKING_URL.key());
          String appTrack = StringUtils.isNotBlank(trackingUrl) ?
              "Tracking URL: " + String.format(trackingUrl, appId) :
              "Application ID: " + appId;
          logger.info(appTrack);
          logStage = 2;
        }
        // Report application state.
        if (logStage == 2 && StringUtils.isNotBlank(appId) &&
            StringUtils.isNotBlank(sessionView.getState())) {
          logger.info("Application report for {} (state: {})", appId,
              sessionView.getState().toUpperCase(Locale.ENGLISH));
          Thread.sleep(2800);
        }

        isActive = SessionState.apply(sessionView.getState()).isActive();
        if (!isActive) {
          // Make sure all logs are printed after the application is finished.
          printSessionLog();
        }
        Thread.sleep(200);
      } catch (ConnectException | ServiceUnavailableException | AuthServerException ce) {
        logger.warn("Please wait, the session {} is recovering.", sessionId);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          break;
        }
      } catch (Exception e) {
        throw new LivyLauncherException(LauncherExitCode.others, e.getMessage(),
            e.getCause());
      }
    }
    return exitCode;
  }

  @Override
  public BaseOptions uploadResources(BaseOptions baseOptions)
      throws IOException {
    BatchOptions batchOptions = (BatchOptions) baseOptions;
    String file = batchOptions.getFile();
    if (StringUtils.isNotBlank(file)) {
      batchOptions.setFile(copyFileToRemote(file));
    }
    super.uploadResources(baseOptions);
    return batchOptions;
  }

  private void printSessionLog(String logType, int from)
      throws ConnectException {
    int size = 100;
    SessionLogResponse sessionLogResponse =
        this.restClient.getSessionLog(from, size, logType);
    List<String> logs = sessionLogResponse.getLog();
    boolean isReadEnd = (from + size) >= sessionLogResponse.getTotal();

    switch (logType) {
      case "stdout":
        fromStdout += logs.size();
        logReadEnd = isReadEnd ? logReadEnd | STDOUT_READ_END
            : logReadEnd & ~STDOUT_READ_END;
        break;
      case "stderr":
        fromStderr += logs.size();
        logReadEnd = isReadEnd ? logReadEnd | STDERR_READ_END
            : logReadEnd & ~STDERR_READ_END;
        break;
      case "yarnDiagnostics":
        fromDiagnostics += logs.size();
        logReadEnd = isReadEnd ? logReadEnd | YARN_DIAGNOSTICS_READ_END
            : logReadEnd & ~YARN_DIAGNOSTICS_READ_END;
        break;
      default:
    }
    for (String log : sessionLogResponse.getLog()) {
      logStage = log.trim().startsWith("tracking URL:") ? 1 : logStage;
      System.err.println(log);
    }
  }

  private void printSessionLog() throws ConnectException {
    printSessionLog("stdout", fromStdout);
    printSessionLog("stderr", fromStderr);
    printSessionLog("yarnDiagnostics", fromDiagnostics);
  }

  @Override
  public void close() {
    if (restClient != null) {
      restClient.stop(this.waitAppCompletion);
    }
    logger.debug("Close session connection.");
    super.close();
  }
}
