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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class InteractiveOptions {

  private String kind;
  private String proxyUser;
  private List<String> jars = new ArrayList<>();
  private List<String> pyFiles = new ArrayList<>();
  private List<String> files = new ArrayList<>();
  private String driverMemory;
  private Integer driverCores;
  private String executorMemory;
  private Integer executorCores;
  private Integer numExecutors;
  private List<String> archives = new ArrayList<>();
  private String queue;
  private String name;
  private Map<String, String> conf = new HashMap<>();
  private int heartbeatTimeoutInSecond;

  public String getKind() {
    return kind;
  }

  public InteractiveOptions setKind(String kind) {
    this.kind = kind;
    return this;
  }

  public String getProxyUser() {
    return proxyUser;
  }

  public InteractiveOptions setProxyUser(String proxyUser) {
    this.proxyUser = proxyUser;
    return this;
  }

  public List<String> getJars() {
    return jars;
  }

  public InteractiveOptions setJars(List<String> jars) {
    this.jars = jars;
    return this;
  }

  public List<String> getPyFiles() {
    return pyFiles;
  }

  public InteractiveOptions setPyFiles(List<String> pyFiles) {
    this.pyFiles = pyFiles;
    return this;
  }

  public List<String> getFiles() {
    return files;
  }

  public InteractiveOptions setFiles(List<String> files) {
    this.files = files;
    return this;
  }

  public String getDriverMemory() {
    return driverMemory;
  }

  public InteractiveOptions setDriverMemory(String driverMemory) {
    this.driverMemory = driverMemory;
    return this;
  }

  public Integer getDriverCores() {
    return driverCores;
  }

  public InteractiveOptions setDriverCores(Integer driverCores) {
    this.driverCores = driverCores;
    return this;
  }

  public String getExecutorMemory() {
    return executorMemory;
  }

  public InteractiveOptions setExecutorMemory(String executorMemory) {
    this.executorMemory = executorMemory;
    return this;
  }

  public Integer getExecutorCores() {
    return executorCores;
  }

  public InteractiveOptions setExecutorCores(Integer executorCores) {
    this.executorCores = executorCores;
    return this;
  }

  public Integer getNumExecutors() {
    return numExecutors;
  }

  public InteractiveOptions setNumExecutors(Integer numExecutors) {
    this.numExecutors = numExecutors;
    return this;
  }

  public List<String> getArchives() {
    return archives;
  }

  public InteractiveOptions setArchives(List<String> archives) {
    this.archives = archives;
    return this;
  }

  public String getQueue() {
    return queue;
  }

  public InteractiveOptions setQueue(String queue) {
    this.queue = queue;
    return this;
  }

  public String getName() {
    return name;
  }

  public InteractiveOptions setName(String name) {
    this.name = name;
    return this;
  }

  public Map<String, String> getConf() {
    return conf;
  }

  public InteractiveOptions setConf(Map<String, String> conf) {
    this.conf = conf;
    return this;
  }

  public int getHeartbeatTimeoutInSecond() {
    return heartbeatTimeoutInSecond;
  }

  public InteractiveOptions setHeartbeatTimeoutInSecond(
      int heartbeatTimeoutInSecond) {
    this.heartbeatTimeoutInSecond = heartbeatTimeoutInSecond;
    return this;
  }
}
