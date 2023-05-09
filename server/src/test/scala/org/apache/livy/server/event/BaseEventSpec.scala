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

import scala.collection.mutable.ArrayBuffer

import org.scalatest.{BeforeAndAfterAll, Suite}

import org.apache.livy.LivyConf
import org.apache.livy.metrics.common.Metrics
import org.apache.livy.rsc.driver.StatementState
import org.apache.livy.sessions.SessionState

trait BaseEventSpec extends Suite with BeforeAndAfterAll with org.scalatest.Matchers {
  override def beforeAll(): Unit = {
    val livyConf = new LivyConf()
      .set(LivyConf.EVENT_LISTENER_CLASSES, "org.apache.livy.server.event.BufferedEventListener")
    Metrics.init(livyConf)
    Events.close()
    Events.init(livyConf)
  }

  override def afterAll(): Unit = {
    Events.close()
    Events.init(new LivyConf())
  }

  def assertSessionEventBuffer(id: Int, expectedEvents: Array[SessionState]): Unit = {
    val eventsBuffer = BufferedEventListener.buffer
      .filter(_.isInstanceOf[SessionEvent])
      .filter(_.asInstanceOf[SessionEvent].id == id)
    eventsBuffer.size should be(expectedEvents.size)

    for (i <- 0 to (eventsBuffer.size - 1)) {
      eventsBuffer(i).asInstanceOf[SessionEvent].state.state should be(expectedEvents(i).state)
    }
  }

  def assertStatementEventBuffer(id: Int, expectedEvents: Array[StatementState]): Unit = {
    val eventsBuffer = BufferedEventListener.buffer
      .filter(_.isInstanceOf[StatementEvent])
      .filter(_.asInstanceOf[StatementEvent].sessionId == id)
    eventsBuffer.size should be(expectedEvents.size)

    for (i <- 0 to (eventsBuffer.size - 1)) {
      eventsBuffer(i).asInstanceOf[StatementEvent].statementState should be(expectedEvents(i))
    }
  }
}

object BufferedEventListener {
  val buffer: ArrayBuffer[Event] = ArrayBuffer()
}

class BufferedEventListener(livyConf: LivyConf) extends EventListener(livyConf) {

  override def start(): Unit = {}

  override def onEvent(event: Event): Unit = {
    BufferedEventListener.buffer += event
  }

  override def stop(): Unit = {}
}
