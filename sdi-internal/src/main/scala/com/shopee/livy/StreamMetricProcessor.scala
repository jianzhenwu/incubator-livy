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

class StreamingMetricProcessor extends ApplicationEnvProcessor with Logging {
  val DEFAULT_METRICS_SEND_INTERVAL = "15"
  val DEFAULT_METRICS_PUSH_URL =
    "https://monitoring.infra.sz.shopee.io/vmagent/646/api/v1/pushgateway"
  val DEFAULT_METRICS_PUSH_TOKEN = "spark-streaming-metrics:9Oc/KEOzhHo="

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    val appConf = applicationEnvContext.appConf
    val metricPushUrl = Option(appConf.get("spark.streaming.metrics.push.url"))
      .map(_.trim)
      .getOrElse(DEFAULT_METRICS_PUSH_URL)
    val metricsPushToken = Option(appConf.get("spark.streaming.metrics.push.token"))
      .map(_.trim)
      .getOrElse(DEFAULT_METRICS_PUSH_TOKEN)

    val metricSendInterval = Option(appConf.get("spark.streaming.metrics.send.interval"))
      .map(_.trim)
      .getOrElse(DEFAULT_METRICS_SEND_INTERVAL)

    appConf.put("spark.streaming.metrics.push.url", metricPushUrl)
    appConf.put("spark.streaming.metrics.push.token", metricsPushToken)
    appConf.put("spark.streaming.extraListeners",
      "org.apache.livy.toolkit.metrics.listener.SparkStreamingListener")

    Try(metricSendInterval.toInt) match {
      case Success(_) => appConf.put("spark.streaming.metrics.send.interval",
        metricSendInterval)
      case _ => {
        appConf.put("spark.streaming.metrics.send.interval", DEFAULT_METRICS_SEND_INTERVAL)
        warn("Invalid conf of spark.streaming.metrics.send.interval, will set it to "
        + DEFAULT_METRICS_SEND_INTERVAL)
      }
    }
  }
}
