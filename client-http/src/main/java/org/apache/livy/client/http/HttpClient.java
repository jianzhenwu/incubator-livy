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

package org.apache.livy.client.http;

import java.io.File;
import java.net.ConnectException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.Job;
import org.apache.livy.JobHandle;
import org.apache.livy.LivyClient;
import org.apache.livy.client.common.HttpMessages;
import org.apache.livy.client.common.Serializer;
import org.apache.livy.client.http.exception.ServiceUnavailableException;
import org.apache.livy.client.http.exception.TimeoutException;
import org.apache.livy.client.http.param.InteractiveOptions;
import org.apache.livy.client.http.param.StatementOptions;
import org.apache.livy.client.http.response.SessionLogResponse;
import org.apache.livy.client.http.response.SessionStateResponse;
import org.apache.livy.client.http.response.StatementResponse;

import static org.apache.livy.client.common.HttpMessages.*;

/**
 * What is currently missing:
 * - monitoring of spark job IDs launched by jobs
 */
public class HttpClient extends AbstractRestClient implements LivyClient {

  private static Logger logger = LoggerFactory.getLogger(HttpClient.class);

  public static final String UPLOAD_FILE = "upload-file";
  public static final String UPLOAD_PYFILE = "upload-pyfile";
  public static final String UPLOAD_JAR = "upload-jar";

  public static final String ADD_FILE = "add-file";
  public static final String ADD_JAR = "add-jar";
  public static final String ADD_PYFILE = "add-pyfile";

  public static final String JAR = "jar";
  public static final String FILE = "file";

  private final Set<String> finishedSet = new HashSet<>(
      Arrays.asList("error", "dead", "killed", "success"));

  private ScheduledExecutorService executor;
  private Serializer serializer;

  private boolean stopped;

  HttpClient(URI uri, HttpConf httpConf) {
    super(uri, httpConf, true);
    this.createSession(null);
  }

  public HttpClient(URI uri, Properties livyConf, InteractiveOptions options) {
    super(uri, new HttpConf(livyConf), true);
    this.createSession(options);
  }

  private void createSession(InteractiveOptions sessionOptions) {
    this.stopped = false;
    try {
      if (this.sessionCreated) {
        conn.post(null, HttpMessages.SessionInfo.class, "/%d/connect", sessionId);
        logger.info("Created Livy session {}", sessionId);
      } else {
        if (sessionOptions != null) {
          this.sessionId = conn.post(sessionOptions, HttpMessages.SessionInfo.class, "/").id;
        } else {
          Map<String, String> sessionConf = new HashMap<>();
          for (Map.Entry<String, String> e : config) {
            sessionConf.put(e.getKey(), e.getValue());
          }

          ClientMessage create = new CreateClientRequest(sessionConf);
          this.sessionId = conn.post(create, SessionInfo.class, "/").id;
        }
        logger.info("Connected to Livy session {}", sessionId);
      }
    } catch (Exception e) {
      throw propagate(e);
    }

    // Because we only have one connection to the server, we don't need more than a single
    // threaded executor here.
    this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "HttpClient-" + sessionId);
        t.setDaemon(true);
        return t;
      }
    });

    this.serializer = new Serializer();
  }

  @Override
  public <T> JobHandle<T> submit(Job<T> job) {
    return sendJob("submit-job", job);
  }

  @Override
  public <T> Future<T> run(Job<T> job) {
    return sendJob("run-job", job);
  }

  @Override
  public synchronized void stop(boolean shutdownContext) {
    if (!stopped) {
      executor.shutdownNow();
      try {
        if (shutdownContext) {
          conn.delete(Map.class, "/%s", sessionId);
        }
      } catch (Exception e) {
        throw propagate(e);
      } finally {
        try {
          conn.close();
        } catch (Exception e) {
          // Ignore.
        }
      }
      stopped = true;
    }
  }

  @Override
  public Future<?> uploadJar(File jar) {
    return uploadResource(jar, "upload-jar", "jar");
  }

  @Override
  public Future<?> addJar(URI uri) {
    return addResource("add-jar", uri);
  }

  @Override
  public Future<?> uploadFile(File file) {
    return uploadResource(file, "upload-file", "file");
  }

  @Override
  public Future<?> addFile(URI uri) {
    return addResource("add-file", uri);
  }

  public void waitUntilSessionStarted() {

    Set<String> sessionLogType =
        new HashSet<>(Arrays.asList("stdout:", "stderr:", "YARN Diagnostics:"));

    long startTime = System.currentTimeMillis();
    long livySessionCreateTimeoutMs =
        this.config.getTimeAsMs(HttpConf.Entry.SESSION_CREATE_TIMEOUT);
    boolean printLog =
        this.config.getBoolean(HttpConf.Entry.SESSION_CREATE_PRINT_LOG);

    int from = 0;
    int size = 100;
    while (true) {
      try {
        // Get session state first, then get full session log.
        SessionStateResponse stateRes = getSessionState();

        // Print session log.
        if (printLog) {
          try {
            SessionLogResponse sessionLogResponse = getSessionLog(from, size);
            List<String> logs = sessionLogResponse.getLog();
            logs.stream().filter(e -> !sessionLogType.contains(e.trim()))
                .forEach(System.err::println);
            from += logs.size();
          } catch (ConnectException | ServiceUnavailableException e) {
            logger.warn("Fail to get session {} log.", sessionId, e);
          }
        }

        if ("idle".equals(stateRes.getState())) {
          try {
            String trackingUrl =
                this.config.get(HttpConf.Entry.SESSION_TRACKING_URL);
            String appId = getApplicationId();
            String appTrack = StringUtils.isNotBlank(trackingUrl) ?
                "Tracking UR: " + String.format(trackingUrl, appId) :
                "Application ID: " + appId;
            logger.info(appTrack);
            break;
          } catch (ConnectException | ServiceUnavailableException e) {
            logger
                .warn("Fail to get applicationId of session {}.", sessionId, e);
          }
        }
        // Exit when session is finished.
        if (finishedSet.contains(stateRes.getState())) {
          throw new RuntimeException(String.format(
              "Fail to create session %d. The final session status is %s.%n",
              this.sessionId, stateRes.getState()));
        }
      } catch (ConnectException | ServiceUnavailableException e) {
        logger.warn("The session {} is retrying.", this.sessionId, e);
      }
      long currentTime = System.currentTimeMillis();
      if ((currentTime - startTime) > livySessionCreateTimeoutMs) {
        throw new TimeoutException(
            "Create livy session timeout " + livySessionCreateTimeoutMs);
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public StatementResponse runStatement(String code) {

    long startTime = System.currentTimeMillis();
    long timeout =
        HttpClient.this.config.getTimeAsMs(HttpConf.Entry.STATEMENT_TIMEOUT);
    long offset = HttpClient.this.config
        .getTimeAsMs(HttpConf.Entry.STATEMENT_POLLING_INTERVAL_OFFSET);
    long step = HttpClient.this.config
        .getTimeAsMs(HttpConf.Entry.STATEMENT_POLLING_INTERVAL_STEP);
    long max = HttpClient.this.config
        .getTimeAsMs(HttpConf.Entry.STATEMENT_POLLING_INTERVAL_MAX);

    Interval interval = new RetryInterval(offset, step, max);

    int finalStatementId =
        new RetryTask<Integer>(startTime, timeout, interval) {
          @Override
          public Integer task() throws ConnectException {
            StatementResponse submitRes = submitStatement(code);
            return submitRes.getId();
          }
        }.run();

    return new RetryTask<StatementResponse>(startTime, timeout, interval) {
      @Override
      public StatementResponse task() throws ConnectException {
        StatementResponse runRes = statementResult(finalStatementId);
        if (runRes.getProgress() == 1) {
          return runRes;
        }
        return null;
      }
    }.run();
  }

  public void addOrUploadResources(InteractiveOptions args) {
    try {
      this.addOrUploadResources(args.getFiles(), ADD_FILE, UPLOAD_FILE, FILE);
      this.addOrUploadResources(args.getPyFiles(), ADD_PYFILE, UPLOAD_PYFILE, FILE);
      this.addOrUploadResources(args.getJars(), ADD_JAR, UPLOAD_JAR, JAR);
      this.addOrUploadResources(args.getArchives(), ADD_FILE, UPLOAD_FILE, FILE);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
  }

  private void addOrUploadResources(List<String> files, String addCommand,
      String uploadCommand, String paramName)
      throws Exception {
    if (files.isEmpty()) {
      return;
    }

    for (String filename : files) {
      URI fileUri = URI.create(filename);
      if (fileUri.getScheme() == null) {
        File file = new File(filename);
        this.uploadResource(file, uploadCommand, paramName).get();
      } else {
        this.addResource(addCommand, fileUri).get();
      }
    }
  }


  private Future<?> uploadResource(final File file, final String command, final String paramName) {
    Callable<Void> task = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        conn.post(file, Void.class,  paramName, "/%d/%s", sessionId, command);
        return null;
      }
    };
    return executor.submit(task);
  }

  private Future<?> addResource(final String command, final URI resource) {
    Callable<Void> task = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        ClientMessage msg = new AddResource(resource.toString());
        conn.post(msg, Void.class, "/%d/%s", sessionId, command);
        return null;
      }
    };
    return executor.submit(task);
  }

  private <T> JobHandleImpl<T> sendJob(final String command, Job<T> job) {
    final ByteBuffer serializedJob = serializer.serialize(job);
    JobHandleImpl<T> handle = new JobHandleImpl<T>(config, conn, sessionId, executor, serializer);
    handle.start(command, serializedJob);
    return handle;
  }

  private StatementResponse statementResult(int statementId)
      throws ConnectException, ServiceUnavailableException {
    try {
      return conn.get(StatementResponse.class, "/%d/statements/%d", sessionId,
          statementId);
    } catch (ConnectException | ServiceUnavailableException ce) {
      throw ce;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private StatementResponse submitStatement(String code)
      throws ConnectException, ServiceUnavailableException{
    try {
      StatementOptions req = new StatementOptions(code);
      return conn
          .post(req, StatementResponse.class, "/%d/statements", sessionId);
    } catch (ConnectException | ServiceUnavailableException ce) {
      throw ce;
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
  }

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
        } catch (ConnectException | ServiceUnavailableException e) {
          logger.warn("Please wait, the session {} is recovering.", sessionId);
        }

        long currentTime = System.currentTimeMillis();
        if (timeout > 0 && currentTime - startTime > timeout) {
          throw new TimeoutException(
              "Timeout with session " + HttpClient.this.sessionId);
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
