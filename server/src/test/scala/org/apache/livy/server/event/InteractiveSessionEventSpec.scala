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

import scala.concurrent.Await
import scala.concurrent.duration._

import org.apache.spark.launcher.SparkLauncher
import org.json4s.DefaultFormats
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.rsc.RSCConf
import org.apache.livy.server.AccessManager
import org.apache.livy.server.interactive.{CreateInteractiveRequest, InteractiveSession}
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.{PySpark, SessionState}
import org.apache.livy.utils.SparkApp

class InteractiveSessionEventSpec extends FunSpec
  with Matchers
  with BeforeAndAfter
  with LivyBaseUnitTestSuite
  with BaseEventSpec {

  private val livyConf = new LivyConf()
  livyConf.set(LivyConf.REPL_JARS, "dummy.jar")
    .set(LivyConf.LIVY_SPARK_VERSION, sys.env("LIVY_SPARK_VERSION"))
    .set(LivyConf.LIVY_SPARK_SCALA_VERSION, sys.env("LIVY_SCALA_VERSION"))

  implicit val formats = DefaultFormats

  private val accessManager = new AccessManager(livyConf)

  private def createSession(
      sessionId: Int,
      sessionStore: SessionStore = mock[SessionStore],
      mockApp: Option[SparkApp] = None): InteractiveSession = {
    assume(sys.env.get("SPARK_HOME").isDefined, "SPARK_HOME is not set.")

    val req = new CreateInteractiveRequest()
    req.kind = PySpark
    req.driverMemory = Some("512m")
    req.driverCores = Some(1)
    req.executorMemory = Some("512m")
    req.executorCores = Some(1)
    req.name = Some("InteractiveSessionSpec")
    req.conf = Map(
      SparkLauncher.DRIVER_EXTRA_CLASSPATH -> sys.props("java.class.path"),
      RSCConf.Entry.LIVY_JARS.key() -> ""
    )
    InteractiveSession.create(sessionId, None, null, None, livyConf, accessManager, req,
      sessionStore, mockApp)
  }

  describe("A Interactive Event process") {
    var sessionStore: SessionStore = null

    before {
      sessionStore = mock[SessionStore]
      BufferedEventListener.buffer.clear()
    }

    it("should receive dead event") {
      val mockApp = mock[SparkApp]
      val session: InteractiveSession = createSession(99, sessionStore, Some(mockApp))
      session.start()

      eventually(timeout(60 seconds), interval(100 millis)) {
        session.state shouldBe (SessionState.Idle)
      }
      Await.ready(session.stop(), 30 seconds)

      assertEventBuffer(99, Array(SessionState.Starting, SessionState.Idle,
        SessionState.ShuttingDown, SessionState.Dead()))
    }
  }
}
