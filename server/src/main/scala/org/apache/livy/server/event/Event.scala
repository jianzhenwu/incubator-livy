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

package org.apache.livy.server.event

import SessionType.SessionType

import org.apache.livy.sessions.SessionState

trait Event {}

case class SessionEvent(
    sessionType: SessionType,
    id: Int,
    name: Option[String],
    appId: Option[String],
    appTag: String,
    owner: String,
    proxyUser: Option[String],
    state: SessionState
  ) extends Event

object SessionType extends Enumeration {
  type SessionType = Value

  val Interactive = Value("Interactive")
  val Batch = Value("Batch")
}
