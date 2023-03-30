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
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.client.common.LauncherConf;
import org.apache.livy.client.http.exception.AuthServerException;
import org.apache.livy.client.http.exception.ServiceUnavailableException;
import org.apache.livy.client.http.param.BatchOptions;
import org.apache.livy.client.http.response.BatchSessionViewResponse;

import static org.apache.livy.client.common.LauncherConf.Entry.SESSION_STOP_RETRY_COUNT;
import static org.apache.livy.client.common.LauncherConf.Entry.SESSION_STOP_RETRY_INTERVAL;

public class BatchRestClient extends AbstractRestClient {

  private static final Logger logger =
      LoggerFactory.getLogger(BatchRestClient.class);

  private LauncherConf launcherConf = null;

  public BatchRestClient(URI uri, Properties livyConf) {
    super(uri, new HttpConf(livyConf), false);
    this.launcherConf = new LauncherConf(livyConf);
  }

  public BatchSessionViewResponse submitBatchJob(BatchOptions batchOptions) {

    try {
      BatchSessionViewResponse sessionView =
          conn.post(batchOptions, BatchSessionViewResponse.class, "");
      this.sessionId = sessionView.getId();
      return sessionView;
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
  }

  public BatchSessionViewResponse getBatchSessionView() throws ConnectException {
    try {
      return conn.get(BatchSessionViewResponse.class, "/%d", this.sessionId);
    } catch (ConnectException | ServiceUnavailableException | AuthServerException ce) {
      throw ce;
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
  }

  @Override
  public void stop(boolean shutdownContext) {

    int retryCountMax = this.launcherConf.getInt(SESSION_STOP_RETRY_COUNT);
    long retryInterval = this.launcherConf.getTimeAsMs(SESSION_STOP_RETRY_INTERVAL);

    int retryCount = 0;
    while (retryCount < retryCountMax) {
      retryCount++;
      try {
        if (shutdownContext) {
          conn.delete(Map.class, "/%s", sessionId);
        }
        break;
      } catch (ConnectException | ServiceUnavailableException | AuthServerException ce) {
        logger.info("Retry {} time to stop session.", retryCount);
        try {
          Thread.sleep(retryInterval);
        } catch (InterruptedException e) {
          break;
        }
      } catch (Exception e) {
        throw propagate(e);
      }
    }
    try {
      conn.close();
    } catch (Exception e) {
      logger.error("An exception occurred when closing the connection.", e);
    }
  }

  public int getSessionId() {
    return sessionId;
  }

}
