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

import java.io.{BufferedReader, FileInputStream, FileWriter, InputStreamReader}
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
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
        select 1
        """.stripMargin)
    } finally {
      writer.close()
    }
    sql
  }

  val splitSql: Path = {
    val sql = Files.createTempFile("livy-split-test", ".sql")
    sql.toFile.deleteOnExit()
    val writer = new FileWriter(sql.toFile)
    try {
      writer.write(
        """
        select split('a;b;c',';') as c1
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

  val outputPath: Path = {
    val log = Files.createTempFile("output-file", ".csv")
    Files.delete(log.toFile.toPath)
    log.toFile.toPath
  }

  describe("A Batch process") {
    Metrics.init(new LivyConf())
    var sessionStore: SessionStore = null

    before {
      sessionStore = mock[SessionStore]
      Events.init(new LivyConf())
    }

    after {
      if (Files.exists(outputPath)) {
        Files.delete(outputPath)
      }
    }

    it("should create a process") {
      val req = new CreateBatchRequest()
      req.file = script.toString
      req.conf = Map("spark.driver.extraClassPath" -> sys.props("java.class.path"))

      val conf = new LivyConf().set(LivyConf.LOCAL_FS_WHITELIST, sys.props("java.io.tmpdir"))
        .set(LivyConf.LIVY_SPARK_VERSION, sys.env("LIVY_SPARK_VERSION"))
      val accessManager = new AccessManager(conf)
      val batch = BatchSession.create(0, None, req, conf, accessManager, null, None, sessionStore)
      batch.start()

      Utils.waitUntil({ () => !batch.state.isActive }, Duration(10, TimeUnit.SECONDS))

      val logs = batch.logLines()
      (batch.state match {
        case SessionState.Success(_) => true
        case _ => false
      }) should be (true)

      batch.logLines() should contain("hello world")
    }

    it("should update appId and appInfo") {
      val conf = new LivyConf()
        .set(LivyConf.LIVY_SPARK_VERSION, sys.env("LIVY_SPARK_VERSION"))
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
        .set(LivyConf.LIVY_SPARK_VERSION, sys.env("LIVY_SPARK_VERSION"))
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
        .set(LivyConf.LIVY_SPARK_VERSION, sys.env("LIVY_SPARK_VERSION"))
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

    it("should save MasterMetadata into session store") {
      val conf = new LivyConf()
        .set(LivyConf.LIVY_SPARK_MASTER, "yarn")
        .set(LivyConf.LIVY_SPARK_MASTER_YARN_IDS, "default,backup")
        .set(LivyConf.LIVY_SPARK_DEFAULT_VERSION, "default")
        .set(LivyConf.LIVY_SPARK_MASTER_HADOOP_CONF_DIR.key + ".default",
          "file:///dummy-path/hadoop-default-conf")
        .set(LivyConf.LIVY_SPARK_MASTER_HADOOP_CONF_DIR.key + ".backup",
          "file:///dummy-path/hadoop-backup-conf")

      val req = new CreateBatchRequest()
      req.conf = Map(
        "spark.livy.master.yarn.id" -> "default"
      )
      val mockApp = mock[SparkApp]
      val accessManager = new AccessManager(conf)

      when(sessionStore.save(Matchers.eq(BatchSession.RECOVERY_SESSION_TYPE), anyObject()))
        .thenAnswer(new Answer[Unit]() {
          override def answer(invocation: InvocationOnMock): Unit = {
            val recoveryMetadata = invocation.getArgumentAt(1, classOf[BatchRecoveryMetadata])
            recoveryMetadata.masterMetadata.masterType should be ("yarn")
            recoveryMetadata.masterMetadata.masterId should be (Option("default"))
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
        "spark.livy.spark_version_name" -> "v3"
      )
      val conf = new LivyConf().set(LivyConf.LOCAL_FS_WHITELIST, sys.props("java.io.tmpdir"))
        .set(LivyConf.LIVY_SPARK_VERSIONS_ALIAS, "v2, v3")
        .set(LivyConf.LIVY_SPARK_DEFAULT_ALIAS_VERSION, "v3")
        .set("livy.server.spark.version.alias.mapping.v2->v2_0", "*")
        .set("livy.server.spark.version.alias.mapping.v3->v3_0", "*")
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

    it("should init spark environment by request spark version and queue") {
      val req = new CreateBatchRequest()
      req.file = script.toString
      req.conf = Map(
        "spark.driver.extraClassPath" -> sys.props("java.class.path"),
        "spark.livy.spark_version_name" -> "v3",
        "spark.yarn.queue" -> "dev"
      )
      val conf = new LivyConf().set(LivyConf.LOCAL_FS_WHITELIST, sys.props("java.io.tmpdir"))
        .set(LivyConf.LIVY_SPARK_VERSIONS_ALIAS, "v2, v3")
        .set(LivyConf.LIVY_SPARK_DEFAULT_ALIAS_VERSION, "v3")
        .set("livy.server.spark.version.alias.mapping.v2->v2_0", "*")
        .set("livy.server.spark.version.alias.mapping.v3->v3_0", "*")
        .set("livy.server.spark.version.alias.mapping.v3->v3_1", "dev")

        .set(LivyConf.LIVY_SPARK_VERSIONS, "v2_0,v3_0,v3_1")
        .set(LivyConf.LIVY_SPARK_VERSION.key + ".v2_0", "2.0")
        .set(LivyConf.LIVY_SPARK_VERSION.key + ".v3_0", "3.0")
        .set(LivyConf.LIVY_SPARK_VERSION.key + ".v3_1", "3.1")
        .set(LivyConf.SPARK_HOME.key + ".v2_0", "file:///dummy-path/spark2")
        .set(LivyConf.SPARK_HOME.key + ".v3_0", "file:///dummy-path/spark3")
        .set(LivyConf.SPARK_HOME.key + ".v3_1", sys.env("SPARK_HOME"))
        .set(LivyConf.LIVY_SPARK_SCALA_VERSION.key + ".v2_0", "2.11")
        .set(LivyConf.LIVY_SPARK_SCALA_VERSION.key + ".v3_0", "2.12")
        .set(LivyConf.LIVY_SPARK_SCALA_VERSION.key + ".v3_1", "2.12")

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
        .set(LivyConf.LIVY_SPARK_VERSIONS_ALIAS, "v2, v3")
        .set(LivyConf.LIVY_SPARK_DEFAULT_ALIAS_VERSION, "v3")
        .set("livy.server.spark.version.alias.mapping.v2->v2_0", "*")
        .set("livy.server.spark.version.alias.mapping.v3->v3_0", "*")
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
        .set(LivyConf.LIVY_SPARK_VERSIONS_ALIAS, "v2, v3")
        .set(LivyConf.LIVY_SPARK_DEFAULT_ALIAS_VERSION, "v3")
        .set("livy.server.spark.version.alias.mapping.v2->v2_0", "*")
        .set("livy.server.spark.version.alias.mapping.v3->v3_0", "*")
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

    it("should failed when unsupported request master yarn id") {
      val req = new CreateBatchRequest()
      req.file = script.toString
      req.conf = Map(
        "spark.driver.extraClassPath" -> sys.props("java.class.path"),
        "spark.livy.master.yarn.id" -> "yarn3"
      )
      val conf = new LivyConf().set(LivyConf.LOCAL_FS_WHITELIST, sys.props("java.io.tmpdir"))
        .set(LivyConf.REPL_JARS, "livy-repl_2.11.jar,livy-repl_2.12.jar")
        .set(LivyConf.LIVY_SPARK_MASTER, "yarn")
        .set(LivyConf.LIVY_SPARK_MASTER_YARN_IDS, "default,backup")
        .set(LivyConf.LIVY_SPARK_MASTER_YARN_DEFAULT_ID, "default")
        .set(LivyConf.LIVY_SPARK_MASTER_HADOOP_CONF_DIR.key + ".default",
          "file:///dummy-path/hadoop-default-conf")
        .set(LivyConf.LIVY_SPARK_MASTER_HADOOP_CONF_DIR.key + ".backup",
          "file:///dummy-path/hadoop-backup-conf")

      val accessManager = new AccessManager(conf)

      val caught =
        intercept[IllegalArgumentException] {
          val batch = BatchSession.create(
            0, None, req, conf, accessManager, null, None, sessionStore)
          batch.start()
        }
      assert(caught.getMessage == s"the master yarn yarn3 is not support")
    }

    it("should set toolkit jars through livy conf") {
      val tookKitJars = Set(
        "dummy.jar",
        "local:///dummy-path/dummy1.jar",
        "file:///dummy-path/dummy2.jar",
        "hdfs:///dummy-path/dummy3.jar")
      val livyConf = new LivyConf(false)
        .set(LivyConf.LIVY_SPARK_VERSION, sys.env("LIVY_SPARK_VERSION"))
        .set(LivyConf.LIVY_SPARK_SCALA_VERSION, "2.12")
        .set(LivyConf.TOOLKIT_JARS, tookKitJars.mkString(","))
      val req = mock[CreateBatchRequest]
      when(req.file).thenReturn(".jar")
      when(req.queue).thenReturn(None)
      val builderConf = BatchSession.prepareBuilderConf(Map.empty, livyConf, None, req)
      // if livy.toolkit.jars are configured in LivyConf, it should be passed to builderConf.
      builderConf(LivyConf.SPARK_JARS).split(",").toSet === tookKitJars
    }

    it("should put spark.yarn.queue though request") {
      val queue = "queue1"
      val livyConf = new LivyConf()
        .set(LivyConf.LIVY_SPARK_VERSION, sys.env("LIVY_SPARK_VERSION"))
      val req = mock[CreateBatchRequest]
      when(req.queue).thenReturn(Some(queue))
      val builderConf = BatchSession.prepareBuilderConf(Map.empty, livyConf, None, req)
      builderConf("spark.yarn.queue") should be (queue)
    }

    it("should set toolkit jars when batch session is created") {
      val batch = createTestBatchSession(createSparkSqlBootstrapRequest(sql))
      batch.start()
      Utils.waitUntil({ () => !batch.state.isActive }, Duration(1, TimeUnit.MINUTES))
      (batch.state match {
        case SessionState.Success(_) => true
        case _ => false
      }) should be (true)
      batch.stopSession()
    }

    it("should execute failure when output path is existed and overwrite is false") {
      assert(!Files.exists(outputPath))
      Files.createFile(outputPath)
      val batch = createTestBatchSession(
        createSparkSqlBootstrapRequest(sql, Option(outputPath), Option(false)))
      batch.start()
      Utils.waitUntil({ () => !batch.state.isActive }, Duration(1, TimeUnit.MINUTES))
      (batch.state match {
        case SessionState.Success(_) => true
        case _ => false
      }) should be (false)
      batch.stopSession()
    }

    it("should write output stream to specified path") {
      assert(!Files.exists(outputPath))
      val batch = createTestBatchSession(
        createSparkSqlBootstrapRequest(sql, Option(outputPath)))
      batch.start()
      Utils.waitUntil({ () => !batch.state.isActive }, Duration(1, TimeUnit.MINUTES))
      (batch.state match {
        case SessionState.Success(_) => true
        case _ => false
      }) should be (true)
      batch.stopSession()
      Files.exists(outputPath) should be (true)
      var reader: BufferedReader = null
      try {
        val inputStream = new FileInputStream(outputPath.toFile)
        reader = new BufferedReader(new InputStreamReader(inputStream))
        var line = reader.readLine
        val array = new ArrayBuffer[String]()
        while (line != null) {
          array += line
          line = reader.readLine()
        }
        array(0) should be ("\"1\"")
        array(1) should be ("\"1\"")
      } finally {
        if (reader != null) reader.close()
      }
    }

    it("should return an array while executing SQL with split()") {
      assert(!Files.exists(outputPath))
      val batch = createTestBatchSession(
        createSparkSqlBootstrapRequest(splitSql, Option(outputPath)))
      batch.start()
      Utils.waitUntil({ () => !batch.state.isActive }, Duration(1, TimeUnit.MINUTES))
      (batch.state match {
        case SessionState.Success(_) => true
        case _ => false
      }) should be (true)
      batch.stopSession()
      Files.exists(outputPath) should be (true)
      var reader: BufferedReader = null
      try {
        val inputStream = new FileInputStream(outputPath.toFile)
        reader = new BufferedReader(new InputStreamReader(inputStream))
        var line = reader.readLine
        val array = new ArrayBuffer[String]()
        while (line != null) {
          array += line
          line = reader.readLine()
        }
        array(0) should be ("\"c1\"")
        array(1) should be ("\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\"")
      } finally {
        if (reader != null) reader.close()
      }
    }

    def createTestBatchSession(req: CreateBatchRequest): BatchSession = {
      val conf = new LivyConf()
        .set(LivyConf.LOCAL_FS_WHITELIST, sys.props("java.io.tmpdir"))
        .set(LivyConf.LIVY_SPARK_VERSIONS_ALIAS, "v2, v3")
        .set(LivyConf.LIVY_SPARK_DEFAULT_ALIAS_VERSION, "v3")
        .set("livy.server.spark.version.alias.mapping.v2->v2_0", "*")
        .set("livy.server.spark.version.alias.mapping.v3->v3_0", "*")
        .set(LivyConf.LIVY_SPARK_VERSIONS, "v3_0")
        .set(LivyConf.LIVY_SPARK_VERSION.key + ".v3_0", "3.0")
        .set(LivyConf.SPARK_HOME.key + ".v3_0", sys.env("SPARK_HOME"))
        .set(LivyConf.LIVY_SPARK_SCALA_VERSION.key + ".v3_0", "2.12")
        .set(LivyConf.LIVY_SPARK_DEFAULT_VERSION, "v3_0")
        .set(LivyConf.LIVY_SPARK_MASTER_YARN_IDS, "default")
        .set(LivyConf.LIVY_SPARK_MASTER_YARN_IDS + "default", "default")
        .set(LivyConf.LIVY_SPARK_MASTER_YARN_DEFAULT_ID, "default")

      val accessManager = new AccessManager(conf)
      BatchSession.create(0, None, req, conf, accessManager, null, None, sessionStore)
    }

    def createSparkSqlBootstrapRequest(
        sql: Path,
        outputPath: Option[Path] = None,
        overwrite: Option[Boolean] = None): CreateBatchRequest = {
      val req = new CreateBatchRequest()
      req.files = List[String](sql.toString)
      req.file = jar.toString
      req.className = Some("org.apache.livy.toolkit.SparkSqlBootstrap")
      req.args = List[String](sql.toString)
      req.conf = Map(
        "spark.driver.extraClassPath" -> sys.props("java.class.path"),
        "spark.livy.spark_version_name" -> "v3",
        "spark.livy.master.yarn.id" -> "default"
      )
      outputPath.foreach(path =>
        req.conf += ("spark.livy.sql.bootstrap.output" -> path.toString))
      overwrite.foreach(v =>
        req.conf += ("spark.livy.sql.bootstrap.output.overwrite" -> v.toString))
      req
    }

    def testRecoverSession(name: Option[String]): Unit = {
      val conf = new LivyConf()
      val req = new CreateBatchRequest()
      val name = Some("Test Batch Session")
      val mockApp = mock[SparkApp]
      val m = BatchRecoveryMetadata(99, name, None, "appTag", null, None,
        conf.serverMetadata(), null)
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
