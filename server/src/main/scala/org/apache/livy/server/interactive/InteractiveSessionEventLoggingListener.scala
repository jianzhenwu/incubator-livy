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

package org.apache.livy.server.interactive

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

import org.apache.livy.LivyConf
import org.apache.livy.server.event._

class InteractiveSessionEventLoggingListener(livyConf: LivyConf)
  extends DistinguishedEventListener(livyConf) {

  implicit val formats = DefaultFormats.preservingEmptyValues

  override def start(): Unit = {
    info(s"${this.getClass.getName} started")
  }

  override def onSessionEvent(event: SessionEvent): Unit = {
    event.sessionType match {
      case SessionType.Interactive => loggingSessionEvent(event)
      case _ =>
    }
  }

  override def onStatementEvent(event: StatementEvent): Unit = {
    event.sessionType match {
      case SessionType.Interactive => loggingStatementEvent(event)
      case _ =>
    }
  }

  /**
   * Write session event to log file.
   */
  private def loggingSessionEvent(event: SessionEvent): Unit = {
    val loggingEvent = InteractiveSessionLoggingEvent(
      event.sessionType.toString, event.id, event.name,
      event.appId, event.appTag, event.owner, event.proxyUser,
      event.state.toString, event.createTime, event.startedTime)

    info(write(loggingEvent))
  }

  /**
   * Write statement event to log file.
   */
  private def loggingStatementEvent(event: StatementEvent): Unit = {
    val loggingEvent = InteractiveSessionLoggingEvent(
      event.sessionType.toString, event.sessionId, event.sessionName,
      event.sessionAppId, event.sessionAppTag, event.sessionOwner, event.sessionProxyUser,
      event.sessionState.toString, event.sessionCreateTime, event.sessionStartedTime,
      Option(event.statementId), Option(event.statementState.toString),
      Option(event.statementStarted), Option(event.statementCompleted))

    info(write(loggingEvent))
  }

  /**
   * The session event format that needs to be written to the log.
   */
  private case class InteractiveSessionLoggingEvent(
      sessionType: String,
      sessionId: Int,
      sessionName: Option[String],
      sessionAppId: Option[String],
      sessionAppTag: String,
      sessionOwner: String,
      sessionProxyUser: Option[String],
      sessionState: String,
      sessionCreateTime: java.lang.Long,
      sessionStartedTime: java.lang.Long,
      statementId: Option[Int] = None,
      statementState: Option[String] = None,
      statementStarted: Option[java.lang.Long] = None,
      statementCompleted: Option[java.lang.Long] = None)

  override def stop(): Unit = {
    info(s"${this.getClass.getName} stopped")
  }
}
