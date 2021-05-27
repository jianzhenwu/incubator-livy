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

import java.io.FileWriter
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.scalatest.{BeforeAndAfter, FunSpec}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf, Utils}
import org.apache.livy.server.AccessManager
import org.apache.livy.server.batch.{BatchSession, CreateBatchRequest}
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.SessionState
import org.apache.livy.utils.Clock

class BatchSessionEventSpec extends FunSpec
  with BeforeAndAfter
  with org.scalatest.Matchers
  with LivyBaseUnitTestSuite
  with BaseEventSpec {

  val script: Path = {
    val script = Files.createTempFile("livy-test", ".py")
    script.toFile.deleteOnExit()
    val writer = new FileWriter(script.toFile)
    try {
      writer.write(
        """
          |print "hello world"
        """.stripMargin)
    } finally {
      writer.close()
    }
    script
  }

  val errorScript: Path = {
    val script = Files.createTempFile("livy-test-error-script", ".py")
    script.toFile.deleteOnExit()
    val writer = new FileWriter(script.toFile)
    try {
      writer.write(
        """
          |exit(1)
        """.stripMargin)
    } finally {
      writer.close()
    }
    script
  }

  val runForeverScript: Path = {
    val script = Files.createTempFile("livy-test-run-forever-script", ".py")
    script.toFile.deleteOnExit()
    val writer = new FileWriter(script.toFile)
    try {
      writer.write(
        """
          |import time
          |while True:
          | time.sleep(1)
        """.stripMargin)
    } finally {
      writer.close()
    }
    script
  }

  describe("A Batch Event process") {
    var sessionStore: SessionStore = null

    before {
      sessionStore = mock[SessionStore]
      BufferedEventListener.buffer.clear()
    }

    it("should receive success event") {
      val req = new CreateBatchRequest()
      req.file = script.toString
      req.conf = Map("spark.driver.extraClassPath" -> sys.props("java.class.path"))

      val conf = new LivyConf().set(LivyConf.LOCAL_FS_WHITELIST, sys.props("java.io.tmpdir"))
      val accessManager = new AccessManager(conf)
      val batch = BatchSession.create(0, None, req, conf, accessManager, null, None, sessionStore)
      batch.start()

      Utils.waitUntil({ () => !batch.state.isActive }, Duration(10, TimeUnit.SECONDS))

      assertEventBuffer(0, Array(SessionState.Starting, SessionState.Running,
        SessionState.Success()))
    }

    it("should receive dead event") {
      val req = new CreateBatchRequest()
      req.file = errorScript.toString
      req.conf = Map("spark.driver.extraClassPath" -> sys.props("java.class.path"))

      val conf = new LivyConf().set(LivyConf.LOCAL_FS_WHITELIST, sys.props("java.io.tmpdir"))
      val accessManager = new AccessManager(conf)
      val batch = BatchSession.create(0, None, req, conf, accessManager, null, None, sessionStore)
      batch.start()

      Utils.waitUntil({ () => !batch.state.isActive }, Duration(10, TimeUnit.SECONDS))

      assertEventBuffer(0, Array(SessionState.Starting, SessionState.Running, SessionState.Dead()))
    }

    it("should receive killed event") {
      val req = new CreateBatchRequest()
      req.file = runForeverScript.toString
      req.conf = Map("spark.driver.extraClassPath" -> sys.props("java.class.path"))

      val conf = new LivyConf().set(LivyConf.LOCAL_FS_WHITELIST, sys.props("java.io.tmpdir"))
      val accessManager = new AccessManager(conf)
      val batch = BatchSession.create(0, None, req, conf, accessManager, null, None, sessionStore)
      batch.start()
      Clock.sleep(2)
      batch.stopSession()

      Utils.waitUntil({ () => !batch.state.isActive }, Duration(10, TimeUnit.SECONDS))

      assertEventBuffer(0, Array(SessionState.Starting, SessionState.Running,
        SessionState.Killed()))
    }
  }
}
