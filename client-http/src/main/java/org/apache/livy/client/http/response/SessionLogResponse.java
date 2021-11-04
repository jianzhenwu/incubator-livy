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
package org.apache.livy.client.http.response;

import java.util.List;

public class SessionLogResponse {

  private int id;
  private int from;
  private int total;
  private List<String> log;

  public int getId() {
    return id;
  }

  public SessionLogResponse setId(int id) {
    this.id = id;
    return this;
  }

  public int getFrom() {
    return from;
  }

  public SessionLogResponse setFrom(int from) {
    this.from = from;
    return this;
  }

  public int getTotal() {
    return total;
  }

  public SessionLogResponse setTotal(int total) {
    this.total = total;
    return this;
  }

  public List<String> getLog() {
    return log;
  }

  public SessionLogResponse setLog(List<String> log) {
    this.log = log;
    return this;
  }
}
