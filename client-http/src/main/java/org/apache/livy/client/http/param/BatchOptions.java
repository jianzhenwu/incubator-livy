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
package org.apache.livy.client.http.param;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class BatchOptions extends BaseOptions {
  private String file;
  private String className;
  private List<String> args = new ArrayList<>();

  public String getFile() {
    return file;
  }

  public BatchOptions setFile(String file) {
    this.file = file;
    return this;
  }

  public String getClassName() {
    return className;
  }

  public BatchOptions setClassName(String className) {
    this.className = className;
    return this;
  }

  public List<String> getArgs() {
    return args;
  }

  public BatchOptions setArgs(List<String> args) {
    this.args = args;
    return this;
  }
}
