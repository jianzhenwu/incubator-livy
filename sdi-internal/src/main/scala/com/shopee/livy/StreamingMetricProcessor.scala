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


package com.shopee.livy

import scala.util.{Success, Try}

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, Logging}

object StreamingMetricProcessor {
  val RSC_CONF_PREFIX = "livy.rsc."

  val STEAMING_METRIC_ENABLED = "spark.streaming.metrics.push.enabled"
  val STRUCTURED_METRIC_ENABLED = "spark.structured.streaming.metrics.push.enabled"
  val PUSH_URL = "spark.metrics.push.url"
  val PUSH_TOKEN = "spark.metrics.push.token"
  val PUSH_INTERVAL = "spark.metrics.send.interval"
  val EXTRA_LISTENERS = "spark.streaming.extraListeners"
  val QUERY_LISTENERS = "spark.sql.streaming.streamingQueryListeners"
  val STEAMING_LISTENER = "org.apache.livy.toolkit.metrics.listener.SparkStreamingListener"
  val STRUCTURED_LISTENER = "org.apache.livy.toolkit.metrics.listener.StructuredStreamingListener"
  val DEFAULT_METRICS_SEND_INTERVAL = "15"
}

class StreamingMetricProcessor extends ApplicationEnvProcessor with Logging {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {

    import StreamingMetricProcessor._

    val appConf = applicationEnvContext.appConf
    val streamingEnabled = appConf.get(STEAMING_METRIC_ENABLED)
    val structuredEnabled = appConf.get(STRUCTURED_METRIC_ENABLED)
    val url = appConf.get(RSC_CONF_PREFIX + PUSH_URL)
    val token = appConf.get(RSC_CONF_PREFIX + PUSH_TOKEN)
    val interval = appConf.get(RSC_CONF_PREFIX + PUSH_INTERVAL)

    Option(streamingEnabled).filter("true".equalsIgnoreCase).foreach(_ => {
      addPushGateWayConfig()
      appConf.put(EXTRA_LISTENERS, STEAMING_LISTENER)
    })
    Option(structuredEnabled).filter("true".equalsIgnoreCase).foreach(_ => {
      addPushGateWayConfig()
      appConf.put(QUERY_LISTENERS, STRUCTURED_LISTENER)
    })

    def addPushGateWayConfig(): Unit = {
      appConf.put(PUSH_URL, url)
      appConf.put(PUSH_TOKEN, token)
      Try(interval.toInt) match {
        case Success(_) => appConf.put(PUSH_INTERVAL, interval)
        case _ =>
          appConf.put(PUSH_INTERVAL, DEFAULT_METRICS_SEND_INTERVAL)
          warn("Invalid conf of spark.streaming.metrics.send.interval, will set it to "
          + DEFAULT_METRICS_SEND_INTERVAL)
      }
    }
  }
}
