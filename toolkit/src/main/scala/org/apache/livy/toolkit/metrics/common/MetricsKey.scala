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

case class MetricsKey private(name: String, description: String)

object MetricsKey {
  /**
   * It's spark batch metrics below.
   */
  val APPLICATION_START_TIME: MetricsKey =
    MetricsKey("application_start_time",
      "Start time of the application.")
  val APPLICATION_END_TIME: MetricsKey =
    MetricsKey("application_end_time",
      "End time of the application.")
  val LAST_APPLICATION_ATTEMPT_ID: MetricsKey =
    MetricsKey("last_application_attempt_id",
      "Id of current application attempt.")
  val FIRST_JOB_START_TIME: MetricsKey =
    MetricsKey("first_job_start_time",
      "Start time of the first job.")
  val TASK_SPECULATIVE_SUCCESS_COUNT: MetricsKey =
    MetricsKey("task_speculative_success_count",
      "Count of successful speculative tasks.")
  val TASK_SPECULATIVE_KILLED_COUNT: MetricsKey =
    MetricsKey("task_speculative_killed_count",
      "Count of killed speculative tasks.")
  val TASK_EXCEPTION_FAILURE_COUNT: MetricsKey =
    MetricsKey("task_exception_failure_count",
      "Count of failed tasks due to exceptions.")
  val TASK_RESULT_LOST_COUNT: MetricsKey =
    MetricsKey("task_result_lost_count",
      "Count of failed tasks due to lost result.")
  val TASK_FETCH_FAILED_COUNT: MetricsKey =
    MetricsKey("task_fetch_failed_count",
      "Count of failed tasks due to fetch failure.")
  val TASK_EXECUTOR_LOST_COUNT: MetricsKey =
    MetricsKey("task_executor_lost_count",
      "Count of failed tasks due to executor lost.")
  val TASK_LOCALITY_PROCESS_COUNT: MetricsKey =
    MetricsKey("task_locality_process_count",
      "Count of task with process locality.")
  val TASK_LOCALITY_NODE_COUNT: MetricsKey =
    MetricsKey("task_locality_node_count",
      "Count of task with node locality.")
  val TASK_LOCALITY_RACK_COUNT: MetricsKey =
    MetricsKey("task_locality_rack_count",
      "Count of task with rack locality.")
  val TASK_LOCALITY_NO_REF_COUNT: MetricsKey =
    MetricsKey("task_locality_no_ref_count",
      "Count of task with no ref locality.")
  val TASK_LOCALITY_ANY_COUNT: MetricsKey =
    MetricsKey("task_locality_any_count",
      "Count of task with any locality.")
  val TOTAL_SHUFFLE_RECORD_READ: MetricsKey =
    MetricsKey("total_shuffle_record_read",
      "Total shuffle record read")
  val TOTAL_SHUFFLE_REMOTE_BYTES_READ: MetricsKey =
    MetricsKey("total_shuffle_remote_bytes_read",
      "Total shuffle remote bytes read")
  val TOTAL_SHUFFLE_LOCAL_BYTES_READ: MetricsKey =
    MetricsKey("total_shuffle_local_bytes_read",
      "Total shuffle local bytes read")
  val TOTAL_SHUFFLE_TOTAL_BYTES_READ: MetricsKey =
    MetricsKey("total_shuffle_total_bytes_read",
      "Total shuffle total bytes read")
  val TOTAL_SHUFFLE_BYTES_WRITTEN: MetricsKey =
    MetricsKey("total_shuffle_bytes_written",
      "Total shuffle bytes written")
  val TOTAL_SHUFFLE_RECORD_WRITTEN: MetricsKey =
    MetricsKey("total_shuffle_record_written",
      "Total shuffle record written")

  /**
   * It's spark streaming metrics below.
   */
  val LAST_COMPLETED_BATCH_PROCESSING_DELAY: MetricsKey =
    MetricsKey("last_completed_batch_processing_delay",
      "processing delay time of last completed batch.")
  val LAST_COMPLETED_BATCH_SCHEDULING_DELAY: MetricsKey =
    MetricsKey("last_completed_batch_scheduling_delay",
      "scheduling delay time of last completed batch")
  val LAST_COMPLETED_BATCH_TOTAL_DELAY: MetricsKey =
    MetricsKey("last_completed_batch_total_delay",
      "total delay time of last completed batch")
  val LAST_RECEIVED_BATCH_RECORDS: MetricsKey =
    MetricsKey("last_received_batch_records",
      "records count of last received batch")
  val RECEIVERS: MetricsKey = MetricsKey("receivers", "receivers")
  val TOTAL_COMPLETED_BATCHES: MetricsKey =
    MetricsKey("total_completed_batches",
      "total count of completed batches")
  val RUNNING_BATCHES: MetricsKey =
    MetricsKey("running_batches",
      "count of running batches")
  val UNPROCESSED_BATCHES: MetricsKey =
    MetricsKey("unprocessed_batches",
      "count of unprocessed batches")
  val WAITING_BATCHES: MetricsKey =
    MetricsKey("waiting_batches",
      "count of waiting batches")
  val TOTAL_PROCESSED_RECORDS: MetricsKey =
    MetricsKey("total_processed_records",
      "count of total processed records")
  val TOTAL_RECEIVED_RECORDS: MetricsKey =
    MetricsKey("total_received_records",
      "count of total received records")
  /**
   * It's structured streaming metrics below.
   */
  val COMPLETED_BATCHES: MetricsKey =
    MetricsKey("completed_batches", "Total number of successful completed batches in job")
  val FAILED_BATCHES: MetricsKey =
    MetricsKey("failed_batches", "Number of failed batches")
  val FINISHED_BATCHES: MetricsKey =
    MetricsKey("finished_batches", "Number of finished batches")
  val NUM_INPUT_ROWS: MetricsKey =
    MetricsKey("num_input_rows",
      "The aggregate(across all sources) number of records processed in job")
  val NUM_OUTPUT_ROWS: MetricsKey =
    MetricsKey("num_output_rows",
      "The number of rows written to the sink in job(-1 for Continuous mode)")
  val INPUT_ROWS_PER_SECOND: MetricsKey =
    MetricsKey("input_rows_per_second",
      "The aggregate(across all sources) rate at which spark is processing data")
  val PROCESSED_ROWS_PER_SECOND: MetricsKey =
    MetricsKey("processed_rows_per_seconds",
      "The aggregate(across all sources) rate at which Spark is processing data")
  val NUM_ROWS_TOTAL: MetricsKey =
    MetricsKey("num_rows_total", "Number of total state rows")
  val NUM_ROWS_UPDATED: MetricsKey =
    MetricsKey("num_rows_updated", "Number of updated state rows")
  val Memory_USED_BYTES: MetricsKey =
    MetricsKey("memory_used_bytes", "Memory used by state")
  val NUM_ROWS_DROPPED_BY_WATERMARK: MetricsKey =
    MetricsKey("num_rows_dropped_by_watermark", "Number of rows which are dropped by watermark")
  val BATCH_DURATION: MetricsKey =
    MetricsKey("batch_duration", "Total number of completed batches in job")
  val TRIGGER_EXECUTION_DURATION: MetricsKey =
    MetricsKey("trigger_execution_duration", "Duration of trigger execution")
  val GET_LATEST_OFFSET_DURATION: MetricsKey =
    MetricsKey("get_latest_offset_duration", "Duration of getting most recent offset")
  val GET_OFFSET_DURATION: MetricsKey =
    MetricsKey("get_offset_duration", "Duration of getting the maximum available offset for source")
  val WAL_COMMIT_DURATION: MetricsKey =
    MetricsKey("wal_commit_duration", "Duration of committing new offset")
  val GET_BATCH_DURATION: MetricsKey =
    MetricsKey("get_batch_duration", "Duration of getting data between the offsets(`start`,`end`)")
  val Query_PLANNING_DURATION: MetricsKey =
    MetricsKey("query_planning_duration", "Duration of building a spark query plan for execution")
  val RUN_CONTINUOUS_DURATION: MetricsKey =
    MetricsKey("run_continuous_duration", "Duration of Execution time about executed plan")
}
