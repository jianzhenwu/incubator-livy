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

import org.apache.livy.toolkit.metrics.common.{Metrics, MetricsKey}

class StructuredStreamingListener
  extends StreamingQueryListener with StructuredStreamingListenerBase {

  initialize()

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    onBaseQueryStarted(event)
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    onBaseQueryProgress(event)
    val progress = event.progress
    gauges.put(MetricsKey.NUM_ROWS_DROPPED_BY_WATERMARK,
      progress.stateOperators.map(_.numRowsDroppedByWatermark).sum)
    Metrics().updateTimer(MetricsKey.BATCH_DURATION, progress.batchDuration, TimeUnit.MILLISECONDS)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    onBaseQueryTerminated(event)
  }

  override def registerAllGauge(): Unit = {
    registerAllGaugeInBase()
    registerGauge(MetricsKey.NUM_ROWS_DROPPED_BY_WATERMARK, 0)
  }
}
