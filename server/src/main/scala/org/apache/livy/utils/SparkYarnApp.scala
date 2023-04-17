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
package org.apache.livy.utils

import java.io.{File, IOException}
import java.util
import java.util.concurrent.{ConcurrentHashMap, TimeoutException}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try
import scala.util.control.NonFatal

import com.google.common.collect.Lists
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, FinalApplicationStatus, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException
import org.apache.hadoop.yarn.util.ConverterUtils

import org.apache.livy.{LivyConf, Logging, MasterMetadata, Utils}
import org.apache.livy.metrics.common.{Metrics, MetricsKey}

object SparkYarnApp extends Logging {

  def init(livyConf: LivyConf, client: Option[YarnClient] = None): Unit = {
    mockYarnClient = client
    masterYarnIds = livyConf.masterYarnIds
    hadoopConfDirs = masterYarnIds.map(id => livyConf.hadoopConfDir(id))
    sessionLeakageCheckInterval = livyConf.getTimeAsMs(LivyConf.YARN_APP_LEAKAGE_CHECK_INTERVAL)
    sessionLeakageCheckTimeout = livyConf.getTimeAsMs(LivyConf.YARN_APP_LEAKAGE_CHECK_TIMEOUT)
    leakedAppsGCThreads.foreach(thread => {
      thread.setDaemon(true)
      thread.setName(s"LeakedAppsGCThread-${thread.getId}")
      thread.start()
    })
  }

  private[utils] val defaultMasterYarnId = "default"

  private var mockYarnClient: Option[YarnClient] = None

  private[utils] var masterYarnIds: Seq[String] = _
  private[utils] var hadoopConfDirs: Seq[String] = _

  lazy val masterYarnIdClientMap: Map[String, YarnClient] = {
    // init default yarn client if master yarn id not configured.
    if (masterYarnIds.isEmpty) {
      val c = YarnClient.createYarnClient()
      c.init(new YarnConfiguration())
      c.start()
      Map(defaultMasterYarnId -> c)
    } else {
      val yarnClients = hadoopConfDirs.map(confDir => {
        val c = YarnClient.createYarnClient()
        if (StringUtils.isNotBlank(confDir)) {
          info(s"Load resources from the hadoop config directory $confDir to create yarn client")
          val conf = new Configuration(false)
          conf.addResource(
            new Path(s"${new File(confDir).getCanonicalPath}/yarn-site.xml"))
          c.init(conf)
        } else {
          throw new IllegalArgumentException(
            s"Hadoop config directory is empty, please check the configured directories")
        }
        c.start()
        c
      })
      masterYarnIds.zip(yarnClients).toMap
    }
  }

  private def getYarnTagToAppIdTimeout(livyConf: LivyConf): FiniteDuration =
    livyConf.getTimeAsMs(LivyConf.YARN_APP_LOOKUP_TIMEOUT) milliseconds

  private def getYarnPollInterval(livyConf: LivyConf): FiniteDuration =
    livyConf.getTimeAsMs(LivyConf.YARN_POLL_INTERVAL) milliseconds

  private def getYarnPollBackoffMaxInterval(livyConf: LivyConf): FiniteDuration =
    livyConf.getTimeAsMs(LivyConf.YARN_POLL_BACKOFF_MAX_INTERVAL) milliseconds

  private[utils] val appType = Set("SPARK").asJava

  lazy val masterYarnIdAppTagsMap: Map[String, ConcurrentHashMap[String, Long]] = {
    if (masterYarnIds.isEmpty) {
      Map(defaultMasterYarnId -> new java.util.concurrent.ConcurrentHashMap[String, Long]())
    } else {
      masterYarnIds.zip(masterYarnIds.map(_ => new ConcurrentHashMap[String, Long]())).toMap
    }
  }

  private[utils] val appStates: java.util.EnumSet[YarnApplicationState] =
    util.EnumSet.allOf(classOf[YarnApplicationState])

  private var sessionLeakageCheckTimeout: Long = _

  private var sessionLeakageCheckInterval: Long = _

  private lazy val leakedAppsGCThreads: Seq[Thread] = mockYarnClient match {
    case Some(client) => Seq(createLeakedAppsGCThread(defaultMasterYarnId, client))
    case None => masterYarnIdClientMap.map(kv => {
      createLeakedAppsGCThread(kv._1, kv._2)
    }).toSeq
  }

  private def getLeakedAppTags(masterYarnId: Option[String]): ConcurrentHashMap[String, Long] = {
    if (masterYarnId.isEmpty) {
      masterYarnIdAppTagsMap(defaultMasterYarnId)
    } else {
      masterYarnIdAppTagsMap.getOrElse(
        masterYarnId.get,
        throw new NoSuchElementException(
          s"Cannot find leaked app tags for master yarn ${masterYarnId.get}"))
    }
  }

  private def createLeakedAppsGCThread(
      masterYarnId: String,
      client: YarnClient): Thread = new Thread() {
    override def run(): Unit = {
      val leakedAppTags = SparkYarnApp.getLeakedAppTags(Some(masterYarnId))
      while (true) {
        if (!leakedAppTags.isEmpty) {
          // kill the app if found it and remove it if exceeding a threshold
          val iter = leakedAppTags.entrySet().iterator()
          val now = System.currentTimeMillis()
          val tagsLowerCase = leakedAppTags.keySet().asScala.map(_.toLowerCase()).asJava
          val apps = client.getApplications(appType, appStates,
            tagsLowerCase).asScala

          while(iter.hasNext) {
            var isRemoved = false
            val entry = iter.next()

            apps.find(_.getApplicationTags.contains(entry.getKey))
              .foreach({ e =>
                info(s"Kill leaked app ${e.getApplicationId}")
                client.killApplication(e.getApplicationId)
                iter.remove()
                isRemoved = true
              })

            if (!isRemoved) {
              if ((now - entry.getValue) > sessionLeakageCheckTimeout) {
                iter.remove()
                info(s"Remove leaked yarn app tag ${entry.getKey}")
              }
            }
          }
        }
        Thread.sleep(sessionLeakageCheckInterval)
      }
    }
  }


}

/**
 * Provide a class to control a Spark application using YARN API.
 *
 * @param appTag An app tag that can unique identify the YARN app.
 * @param appIdOption The appId of the YARN app. If this's None, SparkYarnApp will find it
 *                    using appTag.
 * @param process The spark-submit process launched the YARN application. This is optional.
 *                If it's provided, SparkYarnApp.log() will include its log.
 * @param listener Optional listener for notification of appId discovery and app state changes.
 */
class SparkYarnApp private[utils] (
    appTag: String,
    appIdOption: Option[String],
    masterMetadata: MasterMetadata,
    process: Option[LineBufferedProcess],
    listener: Option[SparkAppListener],
    livyConf: LivyConf,
    yarnClientOption: => Option[YarnClient] = None) // For unit test.
  extends SparkApp
  with Logging {
  import SparkYarnApp._

  private var killed = false
  private val appIdPromise: Promise[ApplicationId] = Promise()
  private[utils] var state: SparkApp.State = SparkApp.State.STARTING
  private var yarnDiagnostics: IndexedSeq[String] = IndexedSeq.empty[String]

  private val yarnClient: YarnClient = yarnClientOption.getOrElse {
    if (masterMetadata.masterId.nonEmpty) {
      info(s"Get yarn client for master yarn: ${masterMetadata.masterId.get}")
      SparkYarnApp.masterYarnIdClientMap.getOrElse(
        masterMetadata.masterId.get,
        throw new NoSuchElementException(
          s"Cannot find yarn client for master yarn ${masterMetadata.masterId.get}"))
    } else {
      info(s"Master yarn is not defined, using default yarn client")
      masterYarnIdClientMap(defaultMasterYarnId)
    }
  }

  override def log(logType: Option[String] = None): IndexedSeq[String] = {
    logType match {
      case Some("stdout") => process.map(_.inputLines).getOrElse(ArrayBuffer.empty[String])
      case Some("stderr") => process.map(_.errorLines).getOrElse(ArrayBuffer.empty[String])
      case Some("yarnDiagnostics") => yarnDiagnostics
      case None => ("stdout: " +: process.map(_.inputLines).getOrElse(ArrayBuffer.empty[String])) ++
        ("\nstderr: " +: process.map(_.errorLines).getOrElse(ArrayBuffer.empty[String])) ++
        ("\nYARN Diagnostics: " +: yarnDiagnostics)
    }
  }

  def searchAppIdFromLog(): Option[String] = {
    info("Searching appId from log.")
    val pattern = "application_[0-9]+_[0-9]+".r
    var applicationId: Option[String] = None
    log().foreach { line =>
      if (applicationId.isEmpty) {
        val matchResult = pattern.findFirstIn(line)
        matchResult match {
          case Some(appId) =>
            applicationId = Some(appId)
            info(s"Searched $appId from log.")
          case _ =>
        }
      }
    }
    applicationId
  }

  def tryKillAtAppMonitorError(): Unit = synchronized {
    try {
      var applicationId: ApplicationId = null
      if (appIdPromise.isCompleted) {
        applicationId = appIdPromise.future.value.get.get
      } else {
        searchAppIdFromLog().foreach(appId => {
          applicationId = ApplicationId.fromString(appId)
        })
      }
      if (applicationId != null) {
        yarnClient.killApplication(applicationId)
        info(s"Killed ${applicationId.toString} when the application monitor threw an exception.")
      }
    } catch {
      case _: Throwable =>
    }
  }

  override def kill(): Unit = synchronized {
    killed = true
    if (isRunning) {
      try {
        val timeout = SparkYarnApp.getYarnTagToAppIdTimeout(livyConf)
        yarnClient.killApplication(Await.result(appIdPromise.future, timeout))
      } catch {
        // We cannot kill the YARN app without the app id.
        // There's a chance the YARN app hasn't been submitted during a livy-server failure.
        // We don't want a stuck session that can't be deleted. Emit a warning and move on.
        case _: TimeoutException | _: InterruptedException | _: IllegalStateException =>
          warn("Deleting a session while its YARN application is not found.")
          yarnAppMonitorThread.interrupt()
      } finally {
        process.foreach(_.destroy())
      }
    }
  }

  private def isProcessErrExit(): Boolean = {
    process.isDefined && !process.get.isAlive && process.get.exitValue() != 0
  }

  private def changeState(newState: SparkApp.State.Value): Unit = {
    if (state != newState) {
      info(s"Change state from ${state.toString} to ${newState.toString}")
      listener.foreach(_.stateChanged(state, newState))
      state = newState
    }
  }

  /**
   * Find the corresponding YARN application id from an application tag.
   *
   * @param appTag The application tag tagged on the target application.
   *               If the tag is not unique, it returns the first application it found.
   *               It will be converted to lower case to match YARN's behaviour.
   * @return ApplicationId or the failure.
   */
  @tailrec
  private def getAppIdFromTag(
      appTag: String,
      pollInterval: Duration,
      deadline: Deadline): ApplicationId = {
    if (isProcessErrExit()) {
      throw new IllegalStateException("spark-submit start failed")
    }
    require(appTag != null && !appTag.isEmpty, "Could not refresh AppId from empty appTag")

    val appTagLowerCase = appTag.toLowerCase()

    // FIXME Should not loop thru all YARN applications but YarnClient doesn't offer an API.
    // Consider calling rmClient in YarnClient directly.
    // applicationTags
    val tags = Set(appTagLowerCase).asJava
    var lastIOE: IOException = null
    val startTime = System.currentTimeMillis()
    val appList = try {
      yarnClient.getApplications(appType, appStates, tags)
    } catch {
      // Caused by YARN failure or network issue, should wait
      case ioe: IOException =>
        lastIOE = ioe
        error(s"Error get application by tags: ${ioe.getMessage}")
        Lists.newArrayList[ApplicationReport]()
    }
    info(s"getApplication by tag $appTag cost ${System.currentTimeMillis() - startTime}")
    val appReport = if (appList.isEmpty) {
       None
    } else {
      Some(appList.get(0))
    }
    // TODO: Clean the below code
    appReport match {
      case Some(app) => app.getApplicationId
      case None =>
        if (deadline.isOverdue) {
          process.foreach(_.destroy())
          getLeakedAppTags(masterMetadata.masterId).put(appTag, System.currentTimeMillis())
          if(lastIOE == null) {
            val appLookupTimeoutSecs = livyConf.getTimeAsMs(LivyConf.YARN_APP_LOOKUP_TIMEOUT) / 1000
            throw new IllegalStateException(s"No YARN application is found with tag" +
              s" $appTagLowerCase in $appLookupTimeoutSecs seconds. " +
              "This may be because 1) spark-submit fail to submit application to YARN; or " +
              "2) YARN cluster doesn't have enough resources to start the application in time. " +
              "Please check Livy log and YARN log to know the details.")
          } else {
            throw lastIOE
          }
        } else {
          Clock.sleep(pollInterval.toMillis)
          getAppIdFromTag(appTagLowerCase, pollInterval, deadline)
        }
    }
  }

  private def getYarnDiagnostics(appReport: ApplicationReport): IndexedSeq[String] = {
    Option(appReport.getDiagnostics)
      .filter(_.nonEmpty)
      .map[IndexedSeq[String]](_.split("\n"))
      .getOrElse(IndexedSeq.empty)
  }

  // Exposed for unit test.
  private[utils] def isRunning: Boolean = {
    state != SparkApp.State.FAILED && state != SparkApp.State.FINISHED &&
      state != SparkApp.State.KILLED
  }

  // Exposed for unit test.
  private[utils] def mapYarnState(
      appId: ApplicationId,
      yarnAppState: YarnApplicationState,
      finalAppStatus: FinalApplicationStatus): SparkApp.State.Value = {
    (yarnAppState, finalAppStatus) match {
      case (YarnApplicationState.NEW, FinalApplicationStatus.UNDEFINED) |
           (YarnApplicationState.NEW_SAVING, FinalApplicationStatus.UNDEFINED) |
           (YarnApplicationState.SUBMITTED, FinalApplicationStatus.UNDEFINED) |
           (YarnApplicationState.ACCEPTED, FinalApplicationStatus.UNDEFINED) =>
        SparkApp.State.STARTING
      case (YarnApplicationState.RUNNING, FinalApplicationStatus.UNDEFINED) |
           (YarnApplicationState.RUNNING, FinalApplicationStatus.SUCCEEDED) =>
        SparkApp.State.RUNNING
      case (YarnApplicationState.FINISHED, FinalApplicationStatus.SUCCEEDED) =>
        SparkApp.State.FINISHED
      case (YarnApplicationState.FAILED, FinalApplicationStatus.FAILED) =>
        SparkApp.State.FAILED
      case (YarnApplicationState.KILLED, FinalApplicationStatus.KILLED) =>
        SparkApp.State.KILLED
      case (state, finalStatus) => // any other combination is invalid, so FAIL the application.
        error(s"Unknown YARN state $state for app $appId with final status $finalStatus.")
        SparkApp.State.FAILED
    }
  }

  // Exposed for unit test.
  // TODO Instead of spawning a thread for every session, create a centralized thread and
  // batch YARN queries.
  private[utils] val yarnAppMonitorThread = Utils.startDaemonThread(s"yarnAppMonitorThread-$this") {
    try {
      info(s"Start up yarnAppMonitorThread with appTag = $appTag")
      // If appId is not known, query YARN by appTag to get it.
      val appId = try {
        appIdOption.map(ConverterUtils.toApplicationId).getOrElse {
          val pollInterval = getYarnPollInterval(livyConf)
          val deadline = getYarnTagToAppIdTimeout(livyConf).fromNow
          getAppIdFromTag(appTag, pollInterval, deadline)
        }
      } catch {
        case e: Exception =>
          appIdPromise.failure(e)
          throw e
      }
      info(s"Success to get applicationId = $appId from tag.")
      appIdPromise.success(appId)

      Thread.currentThread().setName(s"yarnAppMonitorThread-$appId")
      listener.foreach(_.appIdKnown(appId.toString))

      var pollInterval = SparkYarnApp.getYarnPollInterval(livyConf)
      var appInfo = AppInfo()
      while (isRunning) {
        try {
          Clock.sleep(pollInterval.toMillis)

          // Refresh application state
          val appReport = yarnClient.getApplicationReport(appId)
          // reset exponential backoff poll interval
          pollInterval = SparkYarnApp.getYarnPollInterval(livyConf)

          yarnDiagnostics = getYarnDiagnostics(appReport)
          changeState(mapYarnState(
            appReport.getApplicationId,
            appReport.getYarnApplicationState,
            appReport.getFinalApplicationStatus))

          if (isProcessErrExit()) {
            if (killed) {
              changeState(SparkApp.State.KILLED)
            } else {
              changeState(SparkApp.State.FAILED)
            }
          }

          val latestAppInfo = {
            val attempt =
              yarnClient.getApplicationAttemptReport(appReport.getCurrentApplicationAttemptId)
            // No need request container report if attempt application report or
            // container id is not exist.
            val driverLogUrl = if (attempt != null && attempt.getAMContainerId != null) {
              Try(yarnClient.getContainerReport(attempt.getAMContainerId).getLogUrl)
                .toOption
            } else {
              None
            }
            val trackingUrl = livyConf.get(LivyConf.APPLICATION_TRACKING_URL)
            val appTrack = if (StringUtils.isNotBlank(trackingUrl)) {
              String.format(trackingUrl, appReport.getApplicationId)
            } else {
              appReport.getTrackingUrl
            }
            AppInfo(driverLogUrl, Option(appTrack))
          }

          if (appInfo != latestAppInfo) {
            listener.foreach(_.infoChanged(latestAppInfo))
            appInfo = latestAppInfo
          }
        } catch {
          // This exception might be thrown during app is starting up. It's transient.
          case e: ApplicationAttemptNotFoundException =>
          // Workaround YARN-4411: No enum constant FINAL_SAVING from getApplicationAttemptReport()
          case e: IllegalArgumentException =>
            if (e.getMessage.contains("FINAL_SAVING")) {
              debug("Encountered YARN-4411.")
            } else {
              throw e
            }
          // Caused by YARN failure or network issue, should wait
          case ioe: IOException =>
            error(s"Error refresh YARN application state: ${ioe.getMessage}")
            // Exponential backoff
            pollInterval = pollInterval * 2
            if (pollInterval > SparkYarnApp.getYarnPollBackoffMaxInterval(livyConf)) {
              pollInterval = SparkYarnApp.getYarnPollBackoffMaxInterval(livyConf)
            }
        }
      }

      debug(s"$appId $state ${yarnDiagnostics.mkString(" ")}")
    } catch {
      case _: InterruptedException =>
        Metrics().incrementCounter(MetricsKey.YARN_APPLICATION_KILLED_COUNT)
        yarnDiagnostics = ArrayBuffer("Session stopped by user.")
        tryKillAtAppMonitorError()
        changeState(SparkApp.State.KILLED)
      case NonFatal(e) =>
        Metrics().incrementCounter(MetricsKey.YARN_APPLICATION_REFRESH_STATE_FAILED_COUNT)
        error(s"Error whiling refreshing YARN state", e)
        yarnDiagnostics = ArrayBuffer(e.getMessage)
        tryKillAtAppMonitorError()
        changeState(SparkApp.State.FAILED)
    }
  }
}
