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

import java.net.ConnectException;
import java.net.URI;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.livy.client.http.exception.AuthServerException;
import org.apache.livy.client.http.exception.ServiceUnavailableException;
import org.apache.livy.client.http.response.SessionLogResponse;
import org.apache.livy.client.http.response.SessionStateResponse;

import static org.apache.livy.client.http.SessionType.Batches;
import static org.apache.livy.client.http.SessionType.Interactive;

public abstract class AbstractRestClient {

  protected int sessionId;
  protected boolean sessionCreated;
  protected final LivyConnection conn;
  protected HttpConf config;

  private final boolean isInteractive;

  public AbstractRestClient(URI uri, HttpConf httpConf, boolean isInteractive) {

    this.config = httpConf;
    this.isInteractive = isInteractive;

    SessionType sessionType = isInteractive ? Interactive : Batches;

    // If the given URI looks like it refers to an existing session, then try to connect to
    // an existing session. Note this means that any Spark configuration in httpConf will be
    // unused.
    Matcher m = Pattern.compile("(.*)" + "/" + Interactive.getSessionType() + "/([0-9]+)")
        .matcher(uri.getPath());

    try {
      if (m.matches() && this.isInteractive) {
        URI base = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(),
            uri.getPort(), m.group(1), uri.getQuery(), uri.getFragment());
        this.conn = new LivyConnection(base, this.config, sessionType);
        this.sessionId = Integer.parseInt(m.group(2));
        this.sessionCreated = true;
      } else {
        this.conn = new LivyConnection(uri, this.config, sessionType);
      }
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  public SessionLogResponse getSessionLog(int from, int size, String logType)
      throws ConnectException {
    SessionLogResponse sessionLogResponse = null;
    try {
      StringBuilder query = new StringBuilder("from=").append(from)
          .append("&logType=").append(logType);
      if (size != 0) {
        query.append("&size=").append(size);
      }
      sessionLogResponse =
          conn.get(SessionLogResponse.class, "/%d/log", query.toString(),
              this.sessionId);
    } catch (ConnectException | ServiceUnavailableException | AuthServerException ce) {
      throw ce;
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
    return sessionLogResponse;
  }

  public SessionStateResponse getSessionState() throws ConnectException {
    try {
      return conn.get(SessionStateResponse.class, "/%d/state", this.sessionId);
    } catch (ConnectException | ServiceUnavailableException | AuthServerException ce) {
      throw ce;
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
  }

  public String getApplicationId() throws ConnectException{
    try {
      return (String) conn.get(Map.class, "/%d", sessionId).get("appId");
    } catch (ConnectException | ServiceUnavailableException | AuthServerException ce) {
      throw ce;
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
  }

  // stop session and delete from sessionManager
  public void deleteSession() throws ConnectException {
    try {
      conn.delete(Void.class, "/%d", sessionId);
    } catch (ConnectException | ServiceUnavailableException | AuthServerException ce) {
      throw ce;
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
  }

  public int getSessionId() {
    return sessionId;
  }

  public boolean isInteractive() {
    return isInteractive;
  }

  public RuntimeException propagate(Exception cause) {
    if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    } else {
      throw new RuntimeException(cause);
    }
  }

  public abstract void stop(boolean shutdownContext);
}

