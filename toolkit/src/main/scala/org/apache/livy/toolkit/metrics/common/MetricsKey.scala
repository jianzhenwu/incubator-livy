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
  /**
   * It's spark streaming metrics below.
   */
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
  /**
   * It's structured streaming metrics below.
   */
  val COMPLETED_BATCHES =
    MetricsKey("completed_batches", "Total number of successful completed batches in job")
  val FAILED_BATCHES =
    MetricsKey("failed_batches", "Number of failed batches")
  val FINISHED_BATCHES =
    MetricsKey("finished_batches", "Number of finished batches")
  val NUM_INPUT_ROWS =
    MetricsKey("num_input_rows",
      "The aggregate(across all sources) number of records processed in job")
  val NUM_OUTPUT_ROWS =
    MetricsKey("num_output_rows",
      "The number of rows written to the sink in job(-1 for Continuous mode)")
  val INPUT_ROWS_PER_SECOND =
    MetricsKey("input_rows_per_second",
      "The aggregate(across all sources) rate at which spark is processing data")
  val PROCESSED_ROWS_PER_SECOND =
    MetricsKey("processed_rows_per_seconds",
      "The aggregate(across all sources) rate at which Spark is processing data")
  val NUM_ROWS_TOTAL =
    MetricsKey("num_rows_total", "Number of total state rows")
  val NUM_ROWS_UPDATED =
    MetricsKey("num_rows_updated", "Number of updated state rows")
  val Memory_USED_BYTES =
    MetricsKey("memory_used_bytes", "Memory used by state")
  val NUM_ROWS_DROPPED_BY_WATERMARK =
    MetricsKey("num_rows_dropped_by_watermark", "Number of rows which are dropped by watermark")
  val BATCH_DURATION =
    MetricsKey("batch_duration", "Total number of completed batches in job")
  val TRIGGER_EXECUTION_DURATION =
    MetricsKey("trigger_execution_duration", "Duration of trigger execution")
  val GET_LATEST_OFFSET_DURATION =
    MetricsKey("get_latest_offset_duration", "Duration of getting most recent offset")
  val GET_OFFSET_DURATION =
    MetricsKey("get_offset_duration", "Duration of getting the maximum available offset for source")
  val WAL_COMMIT_DURATION =
    MetricsKey("wal_commit_duration", "Duration of committing new offset")
  val GET_BATCH_DURATION =
    MetricsKey("get_batch_duration", "Duration of getting data between the offsets(`start`,`end`)")
  val Query_PLANNING_DURATION =
    MetricsKey("query_planning_duration", "Duration of building a spark query plan for execution")
  val RUN_CONTINUOUS_DURATION =
    MetricsKey("run_continuous_duration", "Duration of Execution time about executed plan")
}
