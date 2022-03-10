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


package org.apache.livy.toolkit.metrics.common

case class MetricsKey private(val name: String, val description: String)

object MetricsKey {
  val LAST_COMPLETED_BATCH_PROCESSING_DELAY =
    MetricsKey("last_completed_batch_processing_delay",
      "processing delay time of last completed batch.")
  val LAST_COMPLETED_BATCH_SCHEDULING_DELAY =
    MetricsKey("last_completed_batch_scheduling_delay",
      "scheduling delay time of last completed batch")
  val LAST_COMPLETED_BATCH_TOTAL_DELAY =
    MetricsKey("last_completed_batch_total_delay",
      "total delay time of last completed batch")
  val LAST_RECEIVED_BATCH_RECORDS =
    MetricsKey("last_received_batch_records",
      "records count of last received batch")
  val RECEIVERS = MetricsKey("receivers", "receivers")
  val TOTAL_COMPLETED_BATCHES =
    MetricsKey("total_completed_batches",
      "total count of completed batches")
  val RUNNING_BATCHES =
    MetricsKey("running_batches",
      "count of running batches")
  val UNPROCESSED_BATCHES =
    MetricsKey("unprocessed_batches",
      "count of unprocessed batches")
  val WAITING_BATCHES =
    MetricsKey("waiting_batches",
      "count of waiting batches")
  val TOTAL_PROCESSED_RECORDS =
    MetricsKey("total_processed_records",
      "count of total processed records")
  val TOTAL_RECEIVED_RECORDS =
    MetricsKey("total_received_records",
      "count of total received records")
}
