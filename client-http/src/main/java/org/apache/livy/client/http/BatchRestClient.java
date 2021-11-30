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

import org.apache.livy.client.http.exception.ServiceUnavailableException;
import org.apache.livy.client.http.param.BatchOptions;
import org.apache.livy.client.http.response.BatchSessionViewResponse;

public class BatchRestClient extends AbstractRestClient {

  public BatchRestClient(URI uri, Properties livyConf) {
    super(uri, new HttpConf(livyConf), false);
  }

  public BatchSessionViewResponse submitBatchJob(BatchOptions batchOptions) {

    try {
      BatchSessionViewResponse sessionView =
          conn.post(batchOptions, BatchSessionViewResponse.class, "/");
      this.sessionId = sessionView.getId();
      return sessionView;
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
  }

  public BatchSessionViewResponse getBatchSessionView() throws ConnectException {
    try {
      return conn.get(BatchSessionViewResponse.class, "/%d", this.sessionId);
    } catch (ConnectException | ServiceUnavailableException ce) {
      throw ce;
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
  }

  @Override
  public void stop(boolean shutdownContext) {
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
  }

  public int getSessionId() {
    return sessionId;
  }

}
