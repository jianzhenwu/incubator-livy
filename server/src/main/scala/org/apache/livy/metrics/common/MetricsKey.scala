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

package org.apache.livy.metrics.common

case class MetricsKey private(val name: String, val description: String)

object MetricsKey {
  val BATCH_SESSION_TOTAL_COUNT =
    MetricsKey("batch_session_total_count", "Total Batch Session Count since Livy Start")
  val BATCH_SESSION_FAILED_COUNT =
    MetricsKey("batch_session_failed_count", "Total Failed Batch Session Count since Livy Start")
  val BATCH_SESSION_ACTIVE_NUM =
    MetricsKey("batch_session_active_num", "Active Batch Session Number")
  val BATCH_SESSION_SUBMITTING_NUM =
    MetricsKey("batch_session_submitting_num", "Submitting Batch Session Number")
  val BATCH_SESSION_START_TIME =
    MetricsKey("batch_session_start_time", "Time to Start Batch Session")
  val BATCH_SESSION_PROCESSING_TIME =
    MetricsKey("batch_session_processing_time", "Time to Process Batch Session(Only Success Job)")

  val INTERACTIVE_SESSION_TOTAL_COUNT = MetricsKey("interactive_session_total_count",
    "Total Interactive Session Count since Livy Start")
  val INTERACTIVE_SESSION_FAILED_COUNT = MetricsKey("interactive_session_failed_count",
    "Total Failed Interactive Session Count since Livy Start")
  val INTERACTIVE_SESSION_ACTIVE_NUM =
    MetricsKey("interactive_session_active_num", "Active Interactive Session Number")
  val INTERACTIVE_SESSION_SUBMITTING_NUM =
    MetricsKey("interactive_session_submitting_num", "Submitting Interactive Session Number")
  val INTERACTIVE_SESSION_START_TIME =
    MetricsKey("interactive_session_start_time", "Time to Start Interactive Session")
  val INTERACTIVE_SESSION_STATEMENT_TOTAL_COUNT =
    MetricsKey("interactive_session_statement_total_count",
      "Total Interactive Session Statement Count since Livy Start")
  val INTERACTIVE_SESSION_STATEMENT_SUCCEED_COUNT =
    MetricsKey("interactive_session_statement_succeed_count",
      "Succeed Thrift Session Statement Count since Livy Start")
  val INTERACTIVE_SESSION_STATEMENT_PROCESSING_TIME =
    MetricsKey("interactive_session_statement_processing_time",
      "Thrift Session Statement Processing Time")

  val THRIFT_SESSION_ACTIVE_NUM =
    MetricsKey("thrift_session_active_num", "Active Thrift Session Number")
  val THRIFT_SESSION_TOTAL_COUNT =
    MetricsKey("thrift_session_total_count", "Total Thrift Session Count since Livy Start")
  val THRIFT_SESSION_SQL_TOTAL_COUNT =
    MetricsKey("thrift_session_sql_total_count", "Total Thrift Session SQL Count since Livy Start")
  val THRIFT_SESSION_SQL_SUCCEED_COUNT = MetricsKey("thrift_session_sql_succeed_count",
    "Succeed Thrift Session SQL Count since Livy Start")
  val THRIFT_SESSION_SQL_PROCESSING_TIME = MetricsKey("thrift_session_sql_processing_time",
    "Thrift Session SQL Processing Time")

  val REST_SESSION_LIST_PROCESSING_TIME = MetricsKey("rest_session_list_processing_time",
    "Time to Process Session List Rest API")
  val REST_SESSION_CREATE_PROCESSING_TIME = MetricsKey("rest_session_create_processing_time",
    "Time to Process Session Create Rest API")
  val REST_SESSION_FOUND_IN_MANAGER_COUNT = MetricsKey("rest_session_found_in_manager_count",
    "Session is found in session manager count")
  val REST_SESSION_FOUND_IN_ALLOCATOR_COUNT = MetricsKey("rest_session_found_in_allocator_count",
    "Session is found in session allocator count")
  val REST_SESSION_FOUND_IN_NOWHERE_COUNT = MetricsKey("rest_session_found_in_nowhere_count",
    "Session is found in session allocator count")

  val YARN_APPLICATION_KILLED_COUNT = MetricsKey("yarn_application_killed_count",
    "Total Killed Yarn Application Count by User")
  val YARN_APPLICATION_REFRESH_STATE_FAILED_COUNT =
    MetricsKey("yarn_application_refresh_state_failed_count",
      "Total Failed Yarn Application Count when Refresh Yarn State")

  val CUSTOMIZE_AUTHENTICATION_ERROR_COUNT =
    MetricsKey("customize_authentication_error_count",
      "Error Count of Called Customize Authentication interface")
  val CUSTOMIZE_AUTHENTICATION_FAILED_COUNT =
    MetricsKey("customize_authentication_failed_count",
      "Failed Count of Called Customize Authentication interface")
  val CUSTOMIZE_AUTHENTICATION_TOTAL_COUNT =
    MetricsKey("customize_authentication_total_count",
      "Total Count of Called Customize Authentication interface")

  val ZK_MANAGER_SYNC_PROCESSING_TIME = MetricsKey("zk_manager_sync_processing_time",
    "Time to sync Zookeeper path")
  val ZK_MANAGER_SYNC_TOTAL_COUNT = MetricsKey("zk_manager_sync_total_count",
    "Total count of synchronized Zookeeper paths")
  val ZK_MANAGER_SYNC_TIMEOUT_COUNT = MetricsKey("zk_manager_sync_timeout_count",
    "Total count of Zookeeper path sync timeout")
}
