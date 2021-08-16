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

package org.apache.livy.rsc;

import io.netty.util.concurrent.Promise;

/**
 * Information about driver process and @{@link ContextInfo}
 */
public class DriverProcessInfo {

  private Promise<ContextInfo> contextInfo;
  private transient Process driverProcess;
  private final ContextLauncher.DriverCallbackTimer driverCallbackTimer;

  public DriverProcessInfo(Promise<ContextInfo> contextInfo, Process driverProcess,
                           ContextLauncher.DriverCallbackTimer driverCallbackTimer) {
    this.contextInfo = contextInfo;
    this.driverProcess = driverProcess;
    this.driverCallbackTimer = driverCallbackTimer;
  }

  public Promise<ContextInfo> getContextInfo() {
    return contextInfo;
  }

  public Process getDriverProcess() {
    return driverProcess;
  }

  public ContextLauncher.DriverCallbackTimer getDriverCallbackTimer() {
    return driverCallbackTimer;
  }
}
