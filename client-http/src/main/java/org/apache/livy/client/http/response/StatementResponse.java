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

import java.util.*;

import org.apache.commons.lang3.StringUtils;

public class StatementResponse {

  private int id;
  private String code;
  private String state;
  private Object output;
  private double progress;
  private long started;
  private long completed;

  public StatementOutput getOutputData() {
    String textFormat = "text/plain";
    String jsonFormat = "application/json";
    StatementOutput statementOutput = new StatementOutput();

    List<List<String>> rows = new ArrayList<>();
    if (output instanceof Map) {
      Map<?, ?> outputMap = (Map<?, ?>) output;
      String status = (String) outputMap.get("status");
      statementOutput.setStatus(status);

      if ("ok".equals(status)) {
        Object data = outputMap.get("data");
        Map<?, ?> dataMap = (Map<?, ?>) data;

        if (dataMap.containsKey(jsonFormat)) {
          Object jsonObject = dataMap.get(jsonFormat);
          Object jsonData = ((Map<?, ?>) jsonObject).get("data");
          List<List<Object>> tmp = (List<List<Object>>) jsonData;
          for (List<Object> rawRow : tmp) {
            List<String> row = new ArrayList<>();
            for (Object o : rawRow) {
              row.add(o != null ? o.toString() : null);
            }
            rows.add(row);
          }
        } else if (dataMap.containsKey(textFormat)) {
          String res = (String) dataMap.get(textFormat);
          if (StringUtils.isNotBlank(res)) {
            rows = Collections.singletonList(Collections.singletonList(res));
          }
        }

      } else if ("error".equals(status)) {
        String errValue = (String) outputMap.get("evalue");
        if (errValue != null) {
          rows.add(Collections.singletonList(errValue));
        }
        StringBuilder builder = new StringBuilder();
        for (Object o : (List<Object>) outputMap.get("traceback")) {
          if (o != null) {
            builder.append(o);
          }
        }
        rows.add(Collections.singletonList(builder.toString()));
      }
    }
    statementOutput.setData(rows);
    return statementOutput;
  }

  public int getId() {
    return id;
  }

  public StatementResponse setId(int id) {
    this.id = id;
    return this;
  }

  public String getCode() {
    return code;
  }

  public StatementResponse setCode(String code) {
    this.code = code;
    return this;
  }

  public String getState() {
    return state;
  }

  public StatementResponse setState(String state) {
    this.state = state;
    return this;
  }

  public Object getOutput() {
    return output;
  }

  public StatementResponse setOutput(Object output) {
    this.output = output;
    return this;
  }

  public double getProgress() {
    return progress;
  }

  public StatementResponse setProgress(double progress) {
    this.progress = progress;
    return this;
  }

  public long getStarted() {
    return started;
  }

  public StatementResponse setStarted(long started) {
    this.started = started;
    return this;
  }

  public long getCompleted() {
    return completed;
  }

  public StatementResponse setCompleted(long completed) {
    this.completed = completed;
    return this;
  }
}
