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

object BatchMetricProcessor {
  val RSC_CONF_PREFIX = "livy.rsc."

  val BATCH_LIVY_METRIC_ENABLED = "spark.batch.metrics.push.enabled"

  val PUSH_URL = "spark.batch.metrics.push.url"
  val PUSH_TOKEN = "spark.batch.metrics.push.token"
  val PUSH_INTERVAL = "spark.batch.metrics.send.interval"
  val EXTRA_LISTENERS = "spark.extraListeners"
  val BATCH_LISTENER = "org.apache.livy.toolkit.metrics.listener.SparkBatchListener"
  val DEFAULT_METRICS_SEND_INTERVAL = "15"
  val SINK_CLASS = "spark.metrics.conf.*.sink.prometheus.class"
  val PROMETHEUS_SINK_PATH = "org.apache.spark.metrics.sink.PrometheusSink"
}

class BatchMetricProcessor extends ApplicationEnvProcessor with Logging {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    import BatchMetricProcessor._

    val appConf = applicationEnvContext.appConf
    val batchMetricsEnabled: String = appConf.get(BATCH_LIVY_METRIC_ENABLED)

    val url = appConf.get(RSC_CONF_PREFIX + PUSH_URL)
    val token = appConf.get(RSC_CONF_PREFIX + PUSH_TOKEN)
    val interval = appConf.get(RSC_CONF_PREFIX + PUSH_INTERVAL)

    val extraListeners = appConf.getOrDefault(EXTRA_LISTENERS, "") match {
      case "" => BATCH_LISTENER
      case userDefinedExtraListeners => userDefinedExtraListeners + "," + BATCH_LISTENER
    }

    Option(batchMetricsEnabled).filter("true".equalsIgnoreCase).foreach(_ => {
      addPushGateWayConfig()
      appConf.put(EXTRA_LISTENERS, extraListeners)
      appConf.put(SINK_CLASS, PROMETHEUS_SINK_PATH)
    })

    def addPushGateWayConfig(): Unit = {
      appConf.put(PUSH_URL, url)
      appConf.put(PUSH_TOKEN, token)
      Try(interval.toInt) match {
        case Success(_) => appConf.put(PUSH_INTERVAL, interval)
        case _ =>
          appConf.put(PUSH_INTERVAL, DEFAULT_METRICS_SEND_INTERVAL)
          warn("Invalid conf of spark.batch.metrics.send.interval, will set it to "
            + DEFAULT_METRICS_SEND_INTERVAL)
      }
    }
  }
}
