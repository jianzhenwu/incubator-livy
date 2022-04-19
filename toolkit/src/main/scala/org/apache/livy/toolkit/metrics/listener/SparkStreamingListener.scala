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

import scala.collection.mutable.HashMap

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.scheduler.{BatchInfo, ReceiverInfo, StreamingListener, StreamingListenerBatchCompleted, StreamingListenerBatchStarted, StreamingListenerBatchSubmitted, StreamingListenerReceiverError, StreamingListenerReceiverStarted, StreamingListenerReceiverStopped, StreamingListenerStreamingStarted}

import org.apache.livy.Logging
import org.apache.livy.toolkit.metrics.MetricsInitializer
import org.apache.livy.toolkit.metrics.common.{Metrics, MetricsKey}

private[metrics] class SparkStreamingListener extends StreamingListener
  with MetricsInitializer with Logging {

  private val waitingBatch = new HashMap[Time, BatchInfo]
  private val runningBatch = new HashMap[Time, BatchInfo]
  private val receiverInfos = new HashMap[Int, ReceiverInfo]

  initialize()

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
    synchronized {
      receiverInfos(receiverStarted.receiverInfo.streamId) = receiverStarted.receiverInfo
      gauges.put(MetricsKey.RECEIVERS, receiverInfos.size)
    }
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
    synchronized {
      receiverInfos(receiverError.receiverInfo.streamId) = receiverError.receiverInfo
      gauges.put(MetricsKey.RECEIVERS, receiverInfos.size)
    }
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {
    synchronized {
      receiverInfos(receiverStopped.receiverInfo.streamId) = receiverStopped.receiverInfo
      gauges.put(MetricsKey.RECEIVERS, receiverInfos.size)
    }
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    synchronized {
      waitingBatch(batchSubmitted.batchInfo.batchTime) = batchSubmitted.batchInfo
      Metrics().incrementCounter(MetricsKey.UNPROCESSED_BATCHES)
      Metrics().incrementCounter(MetricsKey.WAITING_BATCHES)
    }
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    synchronized {
      runningBatch(batchStarted.batchInfo.batchTime) = batchStarted.batchInfo
      Metrics().incrementCounter(MetricsKey.UNPROCESSED_BATCHES)
      waitingBatch.remove(batchStarted.batchInfo.batchTime) match {
        case Some(value) => {
          Metrics().decrementCounter(MetricsKey.WAITING_BATCHES)
          Metrics().decrementCounter(MetricsKey.UNPROCESSED_BATCHES)
        }
        case None =>
      }

      gauges.put(MetricsKey.LAST_RECEIVED_BATCH_RECORDS, batchStarted.batchInfo.numRecords)
      Metrics().incrementCounter(MetricsKey.RUNNING_BATCHES)
      Metrics().incrementCounter(MetricsKey.TOTAL_RECEIVED_RECORDS,
        batchStarted.batchInfo.numRecords)
    }
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    synchronized {
      runningBatch.remove(batchCompleted.batchInfo.batchTime) match {
        case Some(value) => {
          Metrics().decrementCounter(MetricsKey.UNPROCESSED_BATCHES)
          Metrics().decrementCounter(MetricsKey.RUNNING_BATCHES)
        }
        case None =>
      }
      waitingBatch.remove(batchCompleted.batchInfo.batchTime) match {
        case Some(value) => {
          Metrics().decrementCounter(MetricsKey.WAITING_BATCHES)
          Metrics().decrementCounter(MetricsKey.UNPROCESSED_BATCHES)
        }
        case None =>
      }

      Metrics().updateTimer(MetricsKey.LAST_COMPLETED_BATCH_PROCESSING_DELAY,
        batchCompleted.batchInfo.processingDelay.get, TimeUnit.MILLISECONDS)
      Metrics().updateTimer(MetricsKey.LAST_COMPLETED_BATCH_SCHEDULING_DELAY,
        batchCompleted.batchInfo.schedulingDelay.get, TimeUnit.MILLISECONDS)
      Metrics().updateTimer(MetricsKey.LAST_COMPLETED_BATCH_TOTAL_DELAY,
        batchCompleted.batchInfo.totalDelay.get, TimeUnit.MILLISECONDS)
      Metrics().incrementCounter(MetricsKey.TOTAL_COMPLETED_BATCHES)
      Metrics().incrementCounter(MetricsKey.TOTAL_PROCESSED_RECORDS,
        batchCompleted.batchInfo.numRecords)
    }
  }

  override def registerAllGauge(): Unit = {
    registerGauge(MetricsKey.LAST_RECEIVED_BATCH_RECORDS, 0)
    registerGauge(MetricsKey.RECEIVERS, 0)
  }
}
