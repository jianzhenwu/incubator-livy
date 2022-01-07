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

package org.apache.livy.server.batch

import java.io.FileWriter
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.mockito.Matchers
import org.mockito.Matchers.anyObject
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{BeforeAndAfter, FunSpec}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf, Utils}
import org.apache.livy.metrics.common.Metrics
import org.apache.livy.server.AccessManager
import org.apache.livy.server.event.Events
import org.apache.livy.server.interactive.InteractiveSession
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.SessionState
import org.apache.livy.utils.{AppInfo, Clock, SparkApp}

class BatchSessionSpec
  extends FunSpec
  with BeforeAndAfter
  with org.scalatest.Matchers
  with LivyBaseUnitTestSuite {

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

  val sql: Path = {
    val sql = Files.createTempFile("livy-test", ".sql")
    sql.toFile.deleteOnExit()
    val writer = new FileWriter(sql.toFile)
    try {
      writer.write(
        """
        show databases
        """.stripMargin)
    } finally {
      writer.close()
    }
    sql
  }

  val jar: Path = {
    val jar = Files.createTempFile("livy-test", ".jar")
    jar.toFile.toPath
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

  describe("A Batch process") {
    Metrics.init(new LivyConf())
    var sessionStore: SessionStore = null

    before {
      sessionStore = mock[SessionStore]
      Events.init(new LivyConf())
    }

    it("should create a process") {
      val req = new CreateBatchRequest()
      req.file = script.toString
      req.conf = Map("spark.driver.extraClassPath" -> sys.props("java.class.path"))

      val conf = new LivyConf().set(LivyConf.LOCAL_FS_WHITELIST, sys.props("java.io.tmpdir"))
      val accessManager = new AccessManager(conf)
      val batch = BatchSession.create(0, None, req, conf, accessManager, null, None, sessionStore)
      batch.start()

      Utils.waitUntil({ () => !batch.state.isActive }, Duration(10, TimeUnit.SECONDS))
      (batch.state match {
        case SessionState.Success(_) => true
        case _ => false
      }) should be (true)

      batch.logLines() should contain("hello world")
    }

    it("should update appId and appInfo") {
      val conf = new LivyConf()
      val req = new CreateBatchRequest()
      val mockApp = mock[SparkApp]
      val accessManager = new AccessManager(conf)
      val batch = BatchSession.create(
        0, None, req, conf, accessManager, null, None, sessionStore, Some(mockApp))
      batch.start()

      val expectedAppId = "APPID"
      batch.appIdKnown(expectedAppId)
      verify(sessionStore, atLeastOnce()).save(
        Matchers.eq(BatchSession.RECOVERY_SESSION_TYPE), anyObject())
      batch.appId shouldEqual Some(expectedAppId)

      val expectedAppInfo = AppInfo(Some("DRIVER LOG URL"), Some("SPARK UI URL"))
      batch.infoChanged(expectedAppInfo)
      batch.appInfo shouldEqual expectedAppInfo
    }

    it("should end with status killed when batch session was stopped") {
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
      (batch.state match {
        case SessionState.Killed(_) => true
        case _ => false
      }) should be (true)
    }

    it("should save ServerMetadata into session store") {
      val conf = new LivyConf()
      conf.set(LivyConf.SERVER_HOST, "126.0.0.1")
      conf.set(LivyConf.SERVER_PORT, 8999)
      val req = new CreateBatchRequest()
      val mockApp = mock[SparkApp]
      val accessManager = new AccessManager(conf)

      when(sessionStore.save(Matchers.eq(BatchSession.RECOVERY_SESSION_TYPE), anyObject()))
        .thenAnswer(new Answer[Unit]() {
          override def answer(invocation: InvocationOnMock): Unit = {
            val recoveryMetadata = invocation.getArgumentAt(1, classOf[BatchRecoveryMetadata])
            recoveryMetadata.serverMetadata.host should be ("126.0.0.1")
            recoveryMetadata.serverMetadata.port should be (8999)
          }
        })

      val batch = BatchSession.create(
        0, None, req, conf, accessManager, null, None, sessionStore, Some(mockApp))
      batch.start()
      val expectedAppId = "APPID"
      batch.appIdKnown(expectedAppId)

      verify(sessionStore, atLeastOnce()).save(
        Matchers.eq(BatchSession.RECOVERY_SESSION_TYPE), anyObject())
    }

    it("should init spark environment by request spark version") {
      val req = new CreateBatchRequest()
      req.file = script.toString
      req.conf = Map(
        "spark.driver.extraClassPath" -> sys.props("java.class.path"),
        "spark.livy.spark_version_name" -> "v3_0"
      )
      val conf = new LivyConf().set(LivyConf.LOCAL_FS_WHITELIST, sys.props("java.io.tmpdir"))
        .set(LivyConf.LIVY_SPARK_VERSIONS, "v2_0,v3_0")
        .set(LivyConf.LIVY_SPARK_VERSION.key + ".v2_0", "2.0")
        .set(LivyConf.LIVY_SPARK_VERSION.key + ".v3_0", "3.0")
        .set(LivyConf.SPARK_HOME.key + ".v2_0", "file:///dummy-path/spark2")
        .set(LivyConf.SPARK_HOME.key + ".v3_0", sys.env("SPARK_HOME"))
        .set(LivyConf.LIVY_SPARK_SCALA_VERSION.key + ".v2_0", "2.11")
        .set(LivyConf.LIVY_SPARK_SCALA_VERSION.key + ".v3_0", "2.12")

      val accessManager = new AccessManager(conf)
      val batch = BatchSession.create(0, None, req, conf, accessManager, null, None, sessionStore)

      batch.start()

      Utils.waitUntil({ () => !batch.state.isActive }, Duration(10, TimeUnit.SECONDS))
      (batch.state match {
        case SessionState.Success(_) => true
        case _ => false
      }) should be(true)
    }

    it("should init spark environment by default spark version without request spark version") {
      val req = new CreateBatchRequest()
      req.file = script.toString
      req.conf = Map(
        "spark.driver.extraClassPath" -> sys.props("java.class.path")
      )
      val conf = new LivyConf().set(LivyConf.LOCAL_FS_WHITELIST, sys.props("java.io.tmpdir"))
        .set(LivyConf.LIVY_SPARK_VERSIONS, "v2_0,v3_0")
        .set(LivyConf.LIVY_SPARK_VERSION.key + ".v2_0", "2.0")
        .set(LivyConf.LIVY_SPARK_VERSION.key + ".v3_0", "3.0")
        .set(LivyConf.SPARK_HOME.key + ".v2_0", "file:///dummy-path/spark2")
        .set(LivyConf.SPARK_HOME.key + ".v3_0", sys.env("SPARK_HOME"))
        .set(LivyConf.LIVY_SPARK_SCALA_VERSION.key + ".v2_0", "2.11")
        .set(LivyConf.LIVY_SPARK_SCALA_VERSION.key + ".v3_0", "2.12")
        .set(LivyConf.LIVY_SPARK_DEFAULT_VERSION, "v3_0")

      val accessManager = new AccessManager(conf)
      val batch = BatchSession.create(0, None, req, conf, accessManager, null, None, sessionStore)

      batch.start()

      Utils.waitUntil({ () => !batch.state.isActive }, Duration(10, TimeUnit.SECONDS))
      (batch.state match {
        case SessionState.Success(_) => true
        case _ => false
      }) should be(true)
    }

    it("should failed when unsupported request spark version") {
      val req = new CreateBatchRequest()
      req.file = script.toString
      req.conf = Map(
        "spark.driver.extraClassPath" -> sys.props("java.class.path"),
        "spark.livy.spark_version_name" -> "v3.2"
      )
      val conf = new LivyConf().set(LivyConf.LOCAL_FS_WHITELIST, sys.props("java.io.tmpdir"))
        .set(LivyConf.LIVY_SPARK_VERSIONS, "v2_0,v3_0")
        .set(LivyConf.LIVY_SPARK_VERSION.key + ".v2_0", "2.0")
        .set(LivyConf.LIVY_SPARK_VERSION.key + ".v3_0", "3.0")
        .set(LivyConf.SPARK_HOME.key + ".v2_0", "file:///dummy-path/spark2")
        .set(LivyConf.SPARK_HOME.key + ".v3_0", sys.env("SPARK_HOME"))
        .set(LivyConf.LIVY_SPARK_SCALA_VERSION.key + ".v2_0", "2.11")
        .set(LivyConf.LIVY_SPARK_SCALA_VERSION.key + ".v3_0", "2.12")

      val accessManager = new AccessManager(conf)
      val batch = BatchSession.create(0, None, req, conf, accessManager, null, None, sessionStore)

      val caught =
        intercept[IllegalArgumentException] {
          batch.start()
        }
      assert(caught.getMessage == "spark version is not support")
    }

    it("should set toolkit jars through livy conf") {
      val tookKitJars = Set(
        "dummy.jar",
        "local:///dummy-path/dummy1.jar",
        "file:///dummy-path/dummy2.jar",
        "hdfs:///dummy-path/dummy3.jar")
      val livyConf = new LivyConf(false)
        .set(LivyConf.LIVY_SPARK_SCALA_VERSION, "2.12")
        .set(LivyConf.TOOLKIT_JARS, tookKitJars.mkString(","))
      val req = mock[CreateBatchRequest]
      when(req.file).thenReturn(".jar")
      val builderConf = BatchSession.prepareBuilderConf(Map.empty, livyConf, None, req)
      // if livy.toolkit.jars are configured in LivyConf, it should be passed to builderConf.
      builderConf(LivyConf.SPARK_JARS).split(",").toSet === tookKitJars
    }

    it("should set toolkit jars when batch session is created") {
      val conf = new LivyConf()
        .set(LivyConf.LOCAL_FS_WHITELIST, sys.props("java.io.tmpdir"))
        .set(LivyConf.LIVY_SPARK_VERSIONS, "v3_0")
        .set(LivyConf.LIVY_SPARK_VERSION.key + ".v3_0", "3.0")
        .set(LivyConf.SPARK_HOME.key + ".v3_0", sys.env("SPARK_HOME"))
        .set(LivyConf.LIVY_SPARK_SCALA_VERSION.key + ".v3_0", "2.12")
        .set(LivyConf.LIVY_SPARK_DEFAULT_VERSION, "v3_0")

      val accessManager = new AccessManager(conf)

      val req = new CreateBatchRequest()
      req.files = List[String](sql.toString)
      req.file = jar.toString
      req.className = Some("org.apache.livy.toolkit.SparkSqlBootstrap")
      req.args = List[String](sql.toString)

      req.conf = Map(
        "spark.driver.extraClassPath" -> sys.props("java.class.path"),
        "spark.livy.spark_version_name" -> "v3_0"
      )
      val batch = BatchSession.create(0, None, req, conf, accessManager, null, None, sessionStore)
      batch.start()
      Utils.waitUntil({ () => !batch.state.isActive }, Duration(1, TimeUnit.MINUTES))
      (batch.state match {
        case SessionState.Success(_) => true
        case _ => false
      }) should be(true)
    }

    def testRecoverSession(name: Option[String]): Unit = {
      val conf = new LivyConf()
      val req = new CreateBatchRequest()
      val name = Some("Test Batch Session")
      val mockApp = mock[SparkApp]
      val m = BatchRecoveryMetadata(99, name, None, "appTag", null, None, conf.serverMetadata())
      val batch = BatchSession.recover(m, conf, sessionStore, Some(mockApp))

      batch.state shouldBe (SessionState.Recovering)
      batch.name shouldBe (name)

      batch.appIdKnown("appId")
      verify(sessionStore, atLeastOnce()).save(
        Matchers.eq(BatchSession.RECOVERY_SESSION_TYPE), anyObject())
    }

    Seq[Option[String]](None, Some("Test Batch Session"), null)
      .foreach { case name =>
        it(s"should recover session (name = $name)") {
          testRecoverSession(name)
        }
      }
  }
}
