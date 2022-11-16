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
package org.apache.livy.launcher

import org.apache.livy.Logging
import org.apache.livy.client.http.response.CancelStatementResponse

private[launcher] object Signaling extends Logging {

  /**
   * Register a SIGINT handler, that terminates current active statement in session
   * or terminates when no statement are currently running.
   * This makes it possible to interrupt a running shell job by pressing Ctrl+C.
   */
  def cancelOnInterrupt(task: RetryTask[CancelStatementResponse]): Unit = {
    SignalUtils.register("INT") {
      logger.warn("Cancelling current statement, this can take a while. " +
        "Press Ctrl+C again to exit spark session.")
      val cancelRes = task.run
      cancelRes != null && "canceled".equals(cancelRes.getMsg)
    }
  }
}
