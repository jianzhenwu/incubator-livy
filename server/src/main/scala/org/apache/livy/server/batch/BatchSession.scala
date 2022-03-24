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

import java.io.File
import java.lang.ProcessBuilder.Redirect
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Random

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import org.apache.hadoop.fs.Path

import org.apache.livy.{LivyConf, Logging, ServerMetadata, Utils}
import org.apache.livy.metrics.common.{Metrics, MetricsKey}
import org.apache.livy.rsc.RSCConf
import org.apache.livy.server.AccessManager
import org.apache.livy.server.event.{Event, Events, SessionEvent, SessionType}
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.{Session, SessionState}
import org.apache.livy.sessions.Session._
import org.apache.livy.utils.{AppInfo, SparkApp, SparkAppListener, SparkProcessBuilder}

@JsonIgnoreProperties(ignoreUnknown = true)
case class BatchRecoveryMetadata(
    id: Int,
    name: Option[String],
    appId: Option[String],
    appTag: String,
    owner: String,
    proxyUser: Option[String],
    serverMetadata: ServerMetadata,
    version: Int = 1)
  extends RecoveryMetadata {

  @JsonIgnore
  def isServerDeallocatable(): Boolean = {
    appTag == null && (appId == null || appId == None)
  }

  @JsonIgnore
  override def isRecoverable(): Boolean = {
    appTag != null || (appId != null && appId != None)
  }
}

object BatchSession extends Logging {
  val RECOVERY_SESSION_TYPE = "batch"
  // batch session child processes number
  private val bscpn = new AtomicInteger

  def childProcesses(): AtomicInteger = {
    bscpn
  }

  def prepareBuilderConf(conf: Map[String, String], livyConf: LivyConf,
      reqSparkVersion: Option[String], request: CreateBatchRequest): Map[String, String] = {

    val builderConf = mutable.Map[String, String]()
    builderConf ++= conf
    val scalaVersion = if (livyConf.sparkVersions.nonEmpty && reqSparkVersion.isDefined) {
      livyConf.get(LivyConf.LIVY_SPARK_SCALA_VERSION.key + "." + reqSparkVersion.get)
    } else {
      livyConf.get(LivyConf.LIVY_SPARK_SCALA_VERSION)
    }

    def livyJars(livyConf: LivyConf, scalaVersion: String): List[String] = {
      Option(livyConf.get(LivyConf.TOOLKIT_JARS)).map { jars =>
        val regex = """[\w-]+_(\d\.\d\d).*\.jar""".r
        jars.split(",").filter { name => new Path(name).getName match {
          // Filter out unmatched scala jars
          case regex(ver) => ver == scalaVersion
          // Keep all the java jars end with ".jar"
          case _ => name.endsWith(".jar")
        }
        }.toList
      }.getOrElse {
        sys.env.get("LIVY_HOME").map { home =>
          val jars = Option(new File(home, s"toolkit_$scalaVersion-jars"))
            .filter(_.isDirectory())
            .getOrElse(new File(home, s"toolkit/scala-$scalaVersion/target/jars"))
          require(jars.isDirectory, "Cannot find Livy TOOLKIT jars.")
          jars.listFiles().map(_.getAbsolutePath()).toList
        }.getOrElse(List[String]())
      }
    }

    Option(scalaVersion).filter(_.nonEmpty).map { scalaVersion =>
      val list = livyJars(livyConf, scalaVersion)
      if (list.nonEmpty) {
        builderConf.get(LivyConf.SPARK_JARS) match {
          case None =>
            builderConf += (LivyConf.SPARK_JARS -> list.mkString(","))
          case Some(oldList) =>
            val newList = (oldList :: list).mkString(",")
            builderConf += (LivyConf.SPARK_JARS -> newList)
        }
      }
    }

    livyConf.iterator().asScala.foreach { e =>
      val (key, value) = (e.getKey, e.getValue)
      if (key.startsWith(RSCConf.RSC_CONF_PREFIX) && !builderConf.contains(key)) {
        builderConf += (key -> value)
      }
    }

    builderConf.toMap
  }

  def create(
      id: Int,
      name: Option[String],
      request: CreateBatchRequest,
      livyConf: LivyConf,
      accessManager: AccessManager,
      owner: String,
      proxyUser: Option[String],
      sessionStore: SessionStore,
      mockApp: Option[SparkApp] = None): BatchSession = {
    val appTag = s"livy-batch-$id-${Random.alphanumeric.take(8).mkString}"
    val impersonatedUser = accessManager.checkImpersonation(proxyUser, owner)

    def createSparkAppInternal(s: BatchSession): SparkApp = {
      val conf = SparkApp.prepareSparkConf(
        appTag,
        livyConf,
        prepareConf(
          request.conf, request.jars, request.files, request.archives, request.pyFiles, livyConf))
      require(request.file != null, "File is required.")

      val reqSparkVersion = if (request.conf.get("spark.livy.spark_version_name").isDefined) {
        request.conf.get("spark.livy.spark_version_name")
      } else {
        Option(livyConf.get(LivyConf.LIVY_SPARK_DEFAULT_VERSION))
      }
      if (reqSparkVersion.isDefined && !livyConf.sparkVersions.contains(reqSparkVersion.get)) {
          throw new IllegalArgumentException("spark version is not support")
      }

      val builderConf = prepareBuilderConf(conf, livyConf, reqSparkVersion, request)

      val builder = new SparkProcessBuilder(livyConf, reqSparkVersion)
      builder.conf(builderConf)
      builder.username(owner)

      impersonatedUser.foreach(builder.proxyUser)
      request.className.foreach(builder.className)
      request.driverMemory.foreach(builder.driverMemory)
      request.driverCores.foreach(builder.driverCores)
      request.executorMemory.foreach(builder.executorMemory)
      request.executorCores.foreach(builder.executorCores)
      request.numExecutors.foreach(builder.numExecutors)
      request.queue.foreach(builder.queue)
      request.name.foreach(builder.name)

      sessionStore.save(BatchSession.RECOVERY_SESSION_TYPE, s.recoveryMetadata)

      builder.redirectOutput(Redirect.PIPE)
      builder.redirectErrorStream(true)

      val file = resolveURIs(Seq(request.file), livyConf)(0)
      val sparkSubmit = builder.start(Some(file), request.args)

      Utils.startDaemonThread(s"batch-session-process-$id") {
        childProcesses.incrementAndGet()
        try {
          sparkSubmit.waitFor() match {
            case 0 =>
            case exitCode =>
              warn(s"spark-submit exited with code $exitCode")
          }
        } finally {
          childProcesses.decrementAndGet()
        }
      }
      SparkApp.create(appTag, None, Option(sparkSubmit), livyConf, Option(s))
    }

    def createSparkApp(s: BatchSession): SparkApp = {
      try {
        createSparkAppInternal(s)
      } catch {
        case e: Throwable =>
          s.stateChanged(SparkApp.State.STARTING, SparkApp.State.FAILED)
          throw e
      }
    }

    info(s"Creating batch session $id: [owner: $owner, request: $request]")

    new BatchSession(
      id,
      name,
      appTag,
      SessionState.Starting,
      livyConf,
      owner,
      impersonatedUser,
      sessionStore,
      mockApp.map { m => (_: BatchSession) => m }.getOrElse(createSparkApp))
  }

  def recover(
      m: BatchRecoveryMetadata,
      livyConf: LivyConf,
      sessionStore: SessionStore,
      mockApp: Option[SparkApp] = None): BatchSession = {
    new BatchSession(
      m.id,
      m.name,
      m.appTag,
      SessionState.Recovering,
      livyConf,
      m.owner,
      m.proxyUser,
      sessionStore,
      mockApp.map { m => (_: BatchSession) => m }.getOrElse { s =>
        SparkApp.create(m.appTag, m.appId, None, livyConf, Option(s))
      })
  }
}

class BatchSession(
    id: Int,
    name: Option[String],
    appTag: String,
    initialState: SessionState,
    livyConf: LivyConf,
    owner: String,
    override val proxyUser: Option[String],
    sessionStore: SessionStore,
    sparkApp: BatchSession => SparkApp)
  extends Session(id, name, owner, livyConf) with SparkAppListener {
  import BatchSession._

  protected implicit def executor: ExecutionContextExecutor = ExecutionContext.global

  private[this] var _state: SessionState = initialState

  private var app: Option[SparkApp] = None

  triggerSessionEvent()

  override def state: SessionState = _state

  override def logLines(logType: Option[String] = None): IndexedSeq[String] =
    app.map(_.log(logType)).getOrElse(IndexedSeq.empty[String])

  override def start(): Unit = {
    app = Option(sparkApp(this))
  }

  override def stopSession(): Unit = {
    app.foreach(_.kill())
  }

  override def appIdKnown(appId: String): Unit = {
    _appId = Option(appId)
    sessionStore.save(RECOVERY_SESSION_TYPE, recoveryMetadata)
  }

  override def stateChanged(oldState: SparkApp.State, newState: SparkApp.State): Unit = {
    synchronized {
      debug(s"$this state changed from $oldState to $newState")
      newState match {
        case SparkApp.State.RUNNING =>
          _state = SessionState.Running
          if (oldState == SparkApp.State.STARTING) {
            _startedTime = System.currentTimeMillis()
            Metrics().updateTimer(MetricsKey.BATCH_SESSION_START_TIME,
              (_startedTime - _createdTime), TimeUnit.MILLISECONDS)
          }
          info(s"Batch session $id created [appid: ${appId.orNull}, state: ${state.toString}, " +
            s"info: ${appInfo.asJavaMap}]")
        case SparkApp.State.FINISHED =>
          _state = SessionState.Success()
          if (oldState == SparkApp.State.RUNNING) {
            Metrics().updateTimer(MetricsKey.BATCH_SESSION_PROCESSING_TIME,
              (System.currentTimeMillis() - _startedTime), TimeUnit.MILLISECONDS)
          }
        case SparkApp.State.KILLED => {
          _state = SessionState.Killed()
        }
        case SparkApp.State.FAILED => {
          _state = SessionState.Dead()
          Metrics().incrementCounter(MetricsKey.BATCH_SESSION_FAILED_COUNT)
        }
        case _ =>
      }
    }
    triggerSessionEvent()
  }

  override def infoChanged(appInfo: AppInfo): Unit = { this.appInfo = appInfo }

  override def recoveryMetadata: RecoveryMetadata =
    BatchRecoveryMetadata(id, name, appId, appTag, owner, proxyUser, livyConf.serverMetadata())

  private def triggerSessionEvent(): Unit = {
    val event: Event = new SessionEvent(SessionType.Batch, id, name, appId, appTag, owner,
      proxyUser, state)
    Events().notify(event)
  }
}
