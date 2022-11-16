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
package org.apache.livy.launcher;

import java.net.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.client.http.HttpClient;
import org.apache.livy.client.http.exception.AuthServerException;
import org.apache.livy.client.http.exception.ServiceUnavailableException;
import org.apache.livy.client.http.exception.TimeoutException;

public abstract class RetryTask<T> {

  private final Logger logger = LoggerFactory.getLogger(RetryTask.class);

  private final HttpClient restClient;

  private final long startTime;
  private final long timeout;
  private final Interval interval;

  public RetryTask(HttpClient restClient, long startTime, long timeout, Interval interval) {
    this.restClient = restClient;
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
        logger.warn("Please wait, the session {} is recovering.", restClient.getSessionId());
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
