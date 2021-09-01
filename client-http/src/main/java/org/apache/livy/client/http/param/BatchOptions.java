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
public class BatchOptions {
  private String file;
  private String proxyUser;
  private String className;
  private List<String> args = new ArrayList<>();
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

  public String getFile() {
    return file;
  }

  public BatchOptions setFile(String file) {
    this.file = file;
    return this;
  }

  public String getProxyUser() {
    return proxyUser;
  }

  public BatchOptions setProxyUser(String proxyUser) {
    this.proxyUser = proxyUser;
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

  public List<String> getJars() {
    return jars;
  }

  public BatchOptions setJars(List<String> jars) {
    this.jars = jars;
    return this;
  }

  public List<String> getPyFiles() {
    return pyFiles;
  }

  public BatchOptions setPyFiles(List<String> pyFiles) {
    this.pyFiles = pyFiles;
    return this;
  }

  public List<String> getFiles() {
    return files;
  }

  public BatchOptions setFiles(List<String> files) {
    this.files = files;
    return this;
  }

  public String getDriverMemory() {
    return driverMemory;
  }

  public BatchOptions setDriverMemory(String driverMemory) {
    this.driverMemory = driverMemory;
    return this;
  }

  public Integer getDriverCores() {
    return driverCores;
  }

  public BatchOptions setDriverCores(Integer driverCores) {
    this.driverCores = driverCores;
    return this;
  }

  public String getExecutorMemory() {
    return executorMemory;
  }

  public BatchOptions setExecutorMemory(String executorMemory) {
    this.executorMemory = executorMemory;
    return this;
  }

  public Integer getExecutorCores() {
    return executorCores;
  }

  public BatchOptions setExecutorCores(Integer executorCores) {
    this.executorCores = executorCores;
    return this;
  }

  public Integer getNumExecutors() {
    return numExecutors;
  }

  public BatchOptions setNumExecutors(Integer numExecutors) {
    this.numExecutors = numExecutors;
    return this;
  }

  public List<String> getArchives() {
    return archives;
  }

  public BatchOptions setArchives(List<String> archives) {
    this.archives = archives;
    return this;
  }

  public String getQueue() {
    return queue;
  }

  public BatchOptions setQueue(String queue) {
    this.queue = queue;
    return this;
  }

  public String getName() {
    return name;
  }

  public BatchOptions setName(String name) {
    this.name = name;
    return this;
  }

  public Map<String, String> getConf() {
    return conf;
  }

  public BatchOptions setConf(Map<String, String> conf) {
    this.conf = conf;
    return this;
  }
}
