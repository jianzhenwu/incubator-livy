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

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.streaming.StreamingQueryListener

import org.apache.livy.Logging
import org.apache.livy.toolkit.metrics.MetricsInitializer
import org.apache.livy.toolkit.metrics.common.{Metrics, MetricsKey}


trait StructuredStreamingListenerBase extends MetricsInitializer with Logging {

  def onBaseQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
  }

  /**
   *  Metrics[NUM_ROWS_DROPPED_BY_WATERMARK, BATCH_DURATION] are not support in scala-2.11, which
   *  is used by spark2.4.
   */
  def onBaseQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val progress = event.progress
    val opDurations = event.progress.durationMs
    gauges.put(MetricsKey.NUM_INPUT_ROWS, progress.numInputRows)
    gauges.put(MetricsKey.INPUT_ROWS_PER_SECOND, progress.inputRowsPerSecond)
    gauges.put(MetricsKey.PROCESSED_ROWS_PER_SECOND, progress.processedRowsPerSecond)
    gauges.put(MetricsKey.NUM_ROWS_TOTAL, progress.stateOperators.map(_.numRowsTotal).sum)
    gauges.put(MetricsKey.NUM_ROWS_UPDATED, progress.stateOperators.map(_.numRowsUpdated).sum)
    gauges.put(MetricsKey.Memory_USED_BYTES, progress.stateOperators.map(_.memoryUsedBytes).sum)
    Metrics().updateTimer(MetricsKey.TRIGGER_EXECUTION_DURATION,
      opDurations.get("triggerExecution"), TimeUnit.MILLISECONDS)
    Metrics().updateTimer(MetricsKey.GET_LATEST_OFFSET_DURATION,
      opDurations.get("latestOffset"), TimeUnit.MILLISECONDS)
    Metrics().updateTimer(MetricsKey.GET_OFFSET_DURATION,
      opDurations.get("getOffset"), TimeUnit.MILLISECONDS)
    Metrics().updateTimer(MetricsKey.GET_BATCH_DURATION,
      opDurations.get("getBatch"), TimeUnit.MILLISECONDS)
    Metrics().updateTimer(MetricsKey.Query_PLANNING_DURATION,
      opDurations.get("queryPlanning"), TimeUnit.MILLISECONDS)
    Metrics().updateTimer(MetricsKey.WAL_COMMIT_DURATION,
      opDurations.get("walCommit"), TimeUnit.MILLISECONDS)
    Metrics().updateTimer(MetricsKey.RUN_CONTINUOUS_DURATION,
      opDurations.get("runContinuous"), TimeUnit.MILLISECONDS)
  }

  def onBaseQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    event.exception match {
      case Some(_) => Metrics().incrementCounter(MetricsKey.FAILED_BATCHES)
      case None => Metrics().incrementCounter(MetricsKey.COMPLETED_BATCHES)
    }
    Metrics().incrementCounter(MetricsKey.FINISHED_BATCHES)
  }

  def registerAllGaugeInBase(): Unit = {
    registerGauge(MetricsKey.NUM_INPUT_ROWS, 0)
    registerGauge(MetricsKey.INPUT_ROWS_PER_SECOND, 0)
    registerGauge(MetricsKey.PROCESSED_ROWS_PER_SECOND, 0)
    registerGauge(MetricsKey.NUM_ROWS_TOTAL, 0)
    registerGauge(MetricsKey.NUM_ROWS_UPDATED, 0)
    registerGauge(MetricsKey.Memory_USED_BYTES, 0)
    registerGauge(MetricsKey.NUM_ROWS_DROPPED_BY_WATERMARK, 0)
  }
}
