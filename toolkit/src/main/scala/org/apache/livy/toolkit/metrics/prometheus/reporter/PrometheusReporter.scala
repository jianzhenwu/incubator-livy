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
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters.mapAsScalaMapConverter

import com.codahale.metrics._

import org.apache.livy.Logging
import org.apache.livy.toolkit.metrics.prometheus.PushGateway

class PrometheusReporter(
    registry: MetricRegistry,
    output: PushGateway,
    prefix: String,
    filter: MetricFilter)
  extends ScheduledReporter(registry, "prometheus-reporter", filter, TimeUnit.SECONDS,
  TimeUnit.MILLISECONDS) with Closeable with Logging
{

  override def report(gauges: util.SortedMap[String, Gauge[_]],
    counters: util.SortedMap[String, Counter], histograms: util.SortedMap[String, Histogram],
    meters: util.SortedMap[String, Meter], timers: util.SortedMap[String, Timer]): Unit = {
    try {
      if (!output.isConnected()) {
        output.connect()
      }

      gauges.asScala.foreach(x => output.sendGauge(prefixed(x._1), x._2))
      counters.asScala.foreach(x => output.sendCounter(prefixed(x._1), x._2))
      timers.asScala.foreach(timer => output.sendTime(prefixed(timer._1), timer._2))

      output.flush()
      output.close()
    } catch {
      case e: IOException => warn("Unable to report to Prometheus", output, e)
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
