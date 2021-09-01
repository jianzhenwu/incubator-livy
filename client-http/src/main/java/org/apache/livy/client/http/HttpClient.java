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
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import org.apache.livy.Job;
import org.apache.livy.JobHandle;
import org.apache.livy.LivyClient;
import org.apache.livy.client.common.HttpMessages;
import org.apache.livy.client.common.Serializer;
import org.apache.livy.client.http.exception.TimeoutException;
import org.apache.livy.client.http.param.InteractiveOptions;
import org.apache.livy.client.http.param.StatementOptions;
import org.apache.livy.client.http.response.SessionStateResponse;
import org.apache.livy.client.http.response.StatementResponse;

import static org.apache.livy.client.common.HttpMessages.*;

/**
 * What is currently missing:
 * - monitoring of spark job IDs launched by jobs
 */
public class HttpClient extends AbstractRestClient implements LivyClient {

  public static final String UPLOAD_FILE = "upload-file";
  public static final String UPLOAD_PYFILE = "upload-pyfile";
  public static final String UPLOAD_JAR = "upload-jar";

  public static final String ADD_FILE = "add-file";
  public static final String ADD_JAR = "add-jar";
  public static final String ADD_PYFILE = "add-pyfile";

  public static final String JAR = "jar";
  public static final String FILE = "file";

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

  public Future<?> uploadJar(File jar) {
    return uploadResource(jar, "upload-jar", "jar");
  }

  @Override
  public Future<?> addJar(URI uri) {
    return addResource("add-jar", uri);
  }

  public Future<?> uploadFile(File file) {
    return uploadResource(file, "upload-file", "file");
  }

  @Override
  public Future<?> addFile(URI uri) {
    return addResource("add-file", uri);
  }

  public void waitUntilSessionStarted() {
    try {
      Set<String> finishedSet = new HashSet<>(
          Arrays.asList("shutting_down", "error", "dead", "killed"));

      long startTime = System.currentTimeMillis();
      long livySessionCreateTimeoutMs =
          this.config.getTimeAsMs(HttpConf.Entry.SESSION_CREATE_TIMEOUT);

      while (true) {
        SessionStateResponse stateRes =
            conn.get(SessionStateResponse.class, "/%d/state", sessionId);
        if ("idle".equals(stateRes.getState())) {
          break;
        }
        if (finishedSet.contains(stateRes.getState())) {
          throw new RuntimeException(String.format(
              "Fail to create session %d. The final session status is %s.%n",
              this.sessionId, stateRes.getState()));
        }
        Thread.sleep(1000);
        long currentTime = System.currentTimeMillis();
        if ((currentTime - startTime) > livySessionCreateTimeoutMs) {
          throw new TimeoutException("Create livy session timeout "
              + livySessionCreateTimeoutMs);
        }
      }
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  public List<List<String>> runStatement(String code) {
    try {
      StatementOptions req = new StatementOptions(code);
      StatementResponse submitRes =
          conn.post(req, StatementResponse.class, "/%d/statements", sessionId);
      int statementId = submitRes.getId();

      long startTime = System.currentTimeMillis();
      long livyStatementTimeoutMs =
          this.config.getTimeAsMs(HttpConf.Entry.STATEMENT_TIMEOUT);

      while (true) {
        // wait 1 second for running
        Thread.sleep(1000);
        StatementResponse runRes =
            conn.get(StatementResponse.class, "/%d/statements/%d", sessionId,
                statementId);
        if (runRes.getProgress() == 1) {
          return runRes.getOutputData();
        }

        long currentTime = System.currentTimeMillis();
        if (livyStatementTimeoutMs > 0 && currentTime - startTime
            > livyStatementTimeoutMs * 1000L) {
          throw new TimeoutException(
              "Run statement timeout " + livyStatementTimeoutMs);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
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

}
