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

package org.apache.livy.toolkit.metrics.listener

import org.apache.spark.{ExceptionFailure, ExecutorLostFailure, FetchFailed, Success, TaskFailedReason, TaskKilled, TaskResultLost}
import org.apache.spark.scheduler._

import org.apache.livy.Logging
import org.apache.livy.toolkit.metrics.MetricsInitializer
import org.apache.livy.toolkit.metrics.common.{Metrics, MetricsKey}

trait SparkBatchListenerBase extends SparkListener
  with MetricsInitializer with Logging {

  var totalShuffleRecordsRead: Long = 0
  var totalShuffleRemoteBytesRead: Long = 0
  var totalShuffleLocalBytesRead: Long = 0
  var totalShuffleTotalBytesRead: Long = 0
  var totalShuffleBytesWritten: Long = 0
  var totalShuffleRecordsWritten: Long = 0

  override def onApplicationStart(
    applicationStart : org.apache.spark.scheduler.SparkListenerApplicationStart): Unit = {

    gauges.put(MetricsKey.APPLICATION_START_TIME, applicationStart.time)

    applicationStart.appAttemptId match {
      case None =>
      case Some(s) =>
          try {
            gauges.put(MetricsKey.LAST_APPLICATION_ATTEMPT_ID, s.toInt)
          } catch {
            case e: Exception => logger.error(s"Error transforming s to Int: $e")
          }
    }
  }

  override def onApplicationEnd(
    applicationEnd : org.apache.spark.scheduler.SparkListenerApplicationEnd): Unit = {
    gauges.put(MetricsKey.APPLICATION_END_TIME, applicationEnd.time)
  }

  override def onJobStart(jobStart : org.apache.spark.scheduler.SparkListenerJobStart): Unit = {
    if (jobStart.jobId == 0)  gauges.put(MetricsKey.FIRST_JOB_START_TIME, jobStart.time)
  }

  override def onStageCompleted(
    stageCompleted : org.apache.spark.scheduler.SparkListenerStageCompleted) : Unit = {
    totalShuffleRecordsRead +=
      stageCompleted.stageInfo.taskMetrics.shuffleReadMetrics.recordsRead
    gauges.put(MetricsKey.TOTAL_SHUFFLE_RECORD_READ, totalShuffleRecordsRead)
    totalShuffleRemoteBytesRead +=
      stageCompleted.stageInfo.taskMetrics.shuffleReadMetrics.remoteBytesRead
    gauges.put(MetricsKey.TOTAL_SHUFFLE_REMOTE_BYTES_READ, totalShuffleRemoteBytesRead)
    totalShuffleLocalBytesRead +=
      stageCompleted.stageInfo.taskMetrics.shuffleReadMetrics.localBytesRead
    gauges.put(MetricsKey.TOTAL_SHUFFLE_LOCAL_BYTES_READ, totalShuffleLocalBytesRead)
    totalShuffleTotalBytesRead +=
      stageCompleted.stageInfo.taskMetrics.shuffleReadMetrics.totalBytesRead
    gauges.put(MetricsKey.TOTAL_SHUFFLE_TOTAL_BYTES_READ, totalShuffleTotalBytesRead)

    totalShuffleBytesWritten +=
      stageCompleted.stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten
    gauges.put(MetricsKey.TOTAL_SHUFFLE_BYTES_WRITTEN, totalShuffleBytesWritten)
    totalShuffleRecordsWritten +=
      stageCompleted.stageInfo.taskMetrics.shuffleWriteMetrics.recordsWritten
    gauges.put(MetricsKey.TOTAL_SHUFFLE_RECORD_WRITTEN, totalShuffleRecordsWritten)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    synchronized {
      taskStart.taskInfo.taskLocality match {
        case TaskLocality.PROCESS_LOCAL =>
          Metrics().incrementCounter(MetricsKey.TASK_LOCALITY_PROCESS_COUNT)
        case TaskLocality.NODE_LOCAL =>
          Metrics().incrementCounter(MetricsKey.TASK_LOCALITY_NODE_COUNT)
        case TaskLocality.RACK_LOCAL =>
          Metrics().incrementCounter(MetricsKey.TASK_LOCALITY_RACK_COUNT)
        case TaskLocality.NO_PREF =>
          Metrics().incrementCounter(MetricsKey.TASK_LOCALITY_NO_REF_COUNT)
        case TaskLocality.ANY =>
          Metrics().incrementCounter(MetricsKey.TASK_LOCALITY_ANY_COUNT)
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    synchronized {
      taskEnd.reason match {
        case Success =>
          if (taskEnd.taskInfo.speculative) {
            Metrics().incrementCounter(MetricsKey.TASK_SPECULATIVE_SUCCESS_COUNT)
          }
        case t: TaskFailedReason => t match {
          // Task was killed intentionally and needs to be rescheduled.
          case _: TaskKilled =>
           if (taskEnd.taskInfo.speculative) {
              Metrics().incrementCounter(MetricsKey.TASK_SPECULATIVE_KILLED_COUNT)
            }
          // Task failed due to a runtime exception
          case _: ExceptionFailure =>
            Metrics().incrementCounter(MetricsKey.TASK_EXCEPTION_FAILURE_COUNT)
          // The task finished successfully,
          // but the result was lost from the executor's block manager
          // before it was fetched.
          case TaskResultLost =>
            Metrics().incrementCounter(MetricsKey.TASK_RESULT_LOST_COUNT)
          // Task failed to fetch shuffle data from a remote node
          case _: FetchFailed =>
            Metrics().incrementCounter(MetricsKey.TASK_FETCH_FAILED_COUNT)
          // The task failed because the executor that it was running on was lost.
          // This may happen because the task crashed the JVM.
          case _: ExecutorLostFailure =>
            Metrics().incrementCounter(MetricsKey.TASK_EXECUTOR_LOST_COUNT)
          case _ =>
        }
        case _ =>
      }
    }
  }

  override def registerAllGauge(): Unit = {
    registerGauge(MetricsKey.APPLICATION_START_TIME, 0)
    registerGauge(MetricsKey.LAST_APPLICATION_ATTEMPT_ID, 0)
    registerGauge(MetricsKey.APPLICATION_END_TIME, 0)
    registerGauge(MetricsKey.FIRST_JOB_START_TIME, 0)

    registerGauge(MetricsKey.TOTAL_SHUFFLE_RECORD_READ, 0)
    registerGauge(MetricsKey.TOTAL_SHUFFLE_REMOTE_BYTES_READ, 0)
    registerGauge(MetricsKey.TOTAL_SHUFFLE_LOCAL_BYTES_READ, 0)
    registerGauge(MetricsKey.TOTAL_SHUFFLE_TOTAL_BYTES_READ, 0)
    registerGauge(MetricsKey.TOTAL_SHUFFLE_BYTES_WRITTEN, 0)
    registerGauge(MetricsKey.TOTAL_SHUFFLE_RECORD_WRITTEN, 0)
  }
}
