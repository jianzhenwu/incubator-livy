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


package org.apache.livy.toolkit.metrics.prometheus.reporter

import java.io.{Closeable, IOException}
import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters.mapAsScalaMapConverter

import com.codahale.metrics._
import org.apache.spark.SparkEnv

import org.apache.livy.Logging
import org.apache.livy.toolkit.metrics.common.Metrics
import org.apache.livy.toolkit.metrics.metrics2.CodahaleMetrics
import org.apache.livy.toolkit.metrics.prometheus.PushGateway

class PrometheusReporter(
    registry: MetricRegistry,
    var output: PushGateway,
    prefix: String,
    filter: MetricFilter)
  extends ScheduledReporter(registry, "prometheus-reporter", filter, TimeUnit.SECONDS,
  TimeUnit.MILLISECONDS) with Closeable with Logging
{

  @Deprecated
  val STREAMING_METRIC_ENABLED = "spark.streaming.metrics.push.enabled"
  val STREAMING_LIVY_METRIC_ENABLED = "spark.livy.streaming.metrics.push.enabled"
  @Deprecated
  val STRUCTURED_METRIC_ENABLED = "spark.structured.streaming.metrics.push.enabled"
  val STRUCTURED_LIVY_METRIC_ENABLED = "spark.livy.structured.streaming.metrics.push.enabled"

  val BATCH_LIVY_METRIC_ENABLED = "spark.batch.metrics.push.enabled"

  val STREAMING_PUSH_URL = "spark.streaming.metrics.push.url"
  val STREAMING_PUSH_TOKEN = "spark.streaming.metrics.push.token"
  val STREAMING_PUSH_INTERVAL = "spark.streaming.metrics.send.interval"

  val BATCH_PUSH_URL = "spark.batch.metrics.push.url"
  val BATCH_PUSH_TOKEN = "spark.batch.metrics.push.token"
  val BATCH_PUSH_INTERVAL = "spark.batch.metrics.send.interval"

  override def start(period: Long, timeUnit: TimeUnit): Unit = {
    import java.util.concurrent._

    report()

    val ex = new ScheduledThreadPoolExecutor(1)
    ex.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = report()
    }, period, period, timeUnit)
  }

  override def report(gauges: util.SortedMap[String, Gauge[_]],
    counters: util.SortedMap[String, Counter], histograms: util.SortedMap[String, Histogram],
    meters: util.SortedMap[String, Meter], timers: util.SortedMap[String, Timer]): Unit = {

    try {
      if (output == null || !output.isConnected()) {
        val conf = SparkEnv.get.conf
        val appId = conf.getAppId
        val appName = conf.get("spark.app.name")
        val queueName = conf.get("spark.yarn.queue")

        val streamingEnabled = conf.getOption(STREAMING_LIVY_METRIC_ENABLED)
          .getOrElse(conf.getOption(STREAMING_METRIC_ENABLED).getOrElse("false"))
        val structuredEnabled = conf.getOption(STRUCTURED_LIVY_METRIC_ENABLED)
          .getOrElse(conf.getOption(STRUCTURED_METRIC_ENABLED).getOrElse("false"))
        val batchEnabled = conf.getOption(BATCH_LIVY_METRIC_ENABLED).getOrElse("false")

        val (pushUrl, pushToken, pushInterval) =
          (streamingEnabled, structuredEnabled, batchEnabled) match {
          case ("true", _, _) | (_, "true", _) =>
            (conf.get(STREAMING_PUSH_URL),
              conf.get(STREAMING_PUSH_TOKEN),
                conf.get(STREAMING_PUSH_INTERVAL))
          case (_, _, "true") =>
            (conf.get(BATCH_PUSH_URL),
              conf.get(BATCH_PUSH_TOKEN),
                conf.get(BATCH_PUSH_INTERVAL))
          case _ =>
              throw new IllegalArgumentException("No metrics source enabled")
        }

        output = new PushGateway()
          .setAppName(appName)
          .setAppId(appId)
          .setQueueName(queueName)
          .setPushUrl(pushUrl)
          .setPushToken(pushToken)
          .setPushInterval(pushInterval)
          .buildTargetUrl()

        Metrics.init(output)
        val metrics: CodahaleMetrics = Metrics.getMetrics.asInstanceOf[CodahaleMetrics]
        val metricsMap = metrics.metricRegistry.getMetrics.asScala

        metricsMap.foreach {
          case (name, metric) =>
            try {
              registry.register(name, metric)
            } catch {
              case e: java.lang.IllegalArgumentException =>
                warn(s"Unable to register metric $name: $e")
            }
        }

        output.connect()
      }

      gauges.asScala.foreach(x => output.sendGauge(prefixed(x._1), x._2))
      counters.asScala.foreach(x => output.sendCounter(prefixed(x._1), x._2))
      timers.asScala.foreach(timer => output.sendTime(prefixed(timer._1), timer._2))

      output.flush()
      output.close()
    } catch {
      case e: Exception =>
        warn(s"Unable to report to Prometheus", output, e)
    }
  }

  private def prefixed(name: String): String = if (prefix == null) name else prefix + name
}

class PrometheusReporterBuilder() {
  var registry: MetricRegistry = _
  var pushGateway: PushGateway = _
  var prefix = ""
  var filter: MetricFilter = MetricFilter.ALL;

  def setRegistry(registry: MetricRegistry): PrometheusReporterBuilder = {
    this.registry = registry
    this
  }

  def setPushGateway(pushGateway: PushGateway): PrometheusReporterBuilder = {
    this.pushGateway = pushGateway
    this
  }

  def setPrefix(prefix : String): PrometheusReporterBuilder = {
    this.prefix = prefix
    this
  }

  def setFilter(filter: MetricFilter): PrometheusReporterBuilder = {
    this.filter = filter
    this
  }

  def build(): PrometheusReporter = {
    new PrometheusReporter(registry, pushGateway, prefix, filter)
  }
}
