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

public class BatchSessionViewResponse {

  private int id;
  private String name;
  private String owner;
  private String proxyUser;
  private String state;
  private String appId;
  private AppInfo appInfo;
  private List<String> log;
  private String server;

  public boolean hasAppInfoValue() {
    return appInfo != null
        && appInfo.getSparkUiUrl() != null
        && appInfo.getDriverLogUrl() != null;
  }

  public int getId() {
    return id;
  }

  public BatchSessionViewResponse setId(int id) {
    this.id = id;
    return this;
  }

  public String getName() {
    return name;
  }

  public BatchSessionViewResponse setName(String name) {
    this.name = name;
    return this;
  }

  public String getOwner() {
    return owner;
  }

  public BatchSessionViewResponse setOwner(String owner) {
    this.owner = owner;
    return this;
  }

  public String getProxyUser() {
    return proxyUser;
  }

  public BatchSessionViewResponse setProxyUser(String proxyUser) {
    this.proxyUser = proxyUser;
    return this;
  }

  public String getState() {
    return state;
  }

  public BatchSessionViewResponse setState(String state) {
    this.state = state;
    return this;
  }

  public String getAppId() {
    return appId;
  }

  public BatchSessionViewResponse setAppId(String appId) {
    this.appId = appId;
    return this;
  }

  public AppInfo getAppInfo() {
    return appInfo;
  }

  public BatchSessionViewResponse setAppInfo(AppInfo appInfo) {
    this.appInfo = appInfo;
    return this;
  }

  public List<String> getLog() {
    return log;
  }

  public BatchSessionViewResponse setLog(List<String> log) {
    this.log = log;
    return this;
  }

  public String getServer() {
    return server;
  }

  public BatchSessionViewResponse setServer(String server) {
    this.server = server;
    return this;
  }

  public static class AppInfo {

    private String driverLogUrl;
    private String sparkUiUrl;

    public String getDriverLogUrl() {
      return driverLogUrl;
    }

    public AppInfo setDriverLogUrl(String driverLogUrl) {
      this.driverLogUrl = driverLogUrl;
      return this;
    }

    public String getSparkUiUrl() {
      return sparkUiUrl;
    }

    public AppInfo setSparkUiUrl(String sparkUiUrl) {
      this.sparkUiUrl = sparkUiUrl;
      return this;
    }
  }
}
