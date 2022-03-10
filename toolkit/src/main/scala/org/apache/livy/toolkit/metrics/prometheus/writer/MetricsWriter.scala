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


package org.apache.livy.toolkit.metrics.prometheus.writer

import java.io.IOException
import java.util.concurrent.TimeUnit

import com.codahale.metrics._

import org.apache.livy.Logging

class MetricsWriter(writer: PrometheusTextWriter) extends Logging {
  private val this.writer = writer;

  @throws[IOException]
  def writeGauge[T](name: String, gauge: Gauge[T]): Unit = {
    val sanitizedName: String = sanitizeMetricName(name)
    writer.writeHelp(sanitizedName, getHelpMessage(name, gauge))
    writer.writeType(sanitizedName, MetricTypes.GaugeType)
    val obj = gauge.getValue()
    val value = obj match {
      case p: Number => p.doubleValue
      case c: Boolean => if (c) 1 else 0
    }
    writer.writeSample(sanitizedName, emptyMap, value)
  }

  /**
   * Export counter as Prometheus  */
  @throws[IOException]
  def writeCounter(dropwizardName: String, counter: Counter): Unit = {
    val name = sanitizeMetricName(dropwizardName)
    writer.writeHelp(name, getHelpMessage(name, counter))
    writer.writeType(name, MetricTypes.CounterType)
    writer.writeSample(name, emptyMap, counter.getCount)
  }

  @throws[IOException]
  def writeTimer(dropwizardName: String, timer: Timer): Unit = {
    writeSnapshotAndCount(dropwizardName, timer.getSnapshot, timer.getCount,
      1.0D / TimeUnit.SECONDS.toNanos(1L), MetricTypes.SummaryType,
      getHelpMessage(dropwizardName, timer))
    writeMetered(dropwizardName, timer)
  }

  @throws[IOException]
  private def writeSnapshotAndCount(dropwizardName: String, snapShot: Snapshot, count: Long,
    factor: Double, metricType: MetricType, helpMessage: String): Unit = {
    val name = sanitizeMetricName(dropwizardName)
    writer.writeHelp(name, helpMessage)
    writer.writeType(name, metricType)
    writer.writeSample(name, Map("quantile" -> "0.5"),
      TimeUnit.NANOSECONDS.toMillis(snapShot.getMedian.toLong))
    writer.writeSample(name, Map("quantile" -> "0.75"),
      TimeUnit.NANOSECONDS.toMillis(snapShot.get75thPercentile.toLong))
    writer.writeSample(name, Map("quantile" -> "0.95"),
      TimeUnit.NANOSECONDS.toMillis(snapShot.get95thPercentile.toLong))
    writer.writeSample(name, Map("quantile" -> "0.98"),
      TimeUnit.NANOSECONDS.toMillis(snapShot.get98thPercentile.toLong))
    writer.writeSample(name, Map("quantile" -> "0.99"),
      TimeUnit.NANOSECONDS.toMillis(snapShot.get99thPercentile.toLong))
    writer.writeSample(name, Map("quantile" -> "0.999"),
      TimeUnit.NANOSECONDS.toMillis(snapShot.get999thPercentile.toLong))
    writer.writeSample(name + "_min", emptyMap,
      TimeUnit.NANOSECONDS.toMillis(snapShot.getMin))
    writer.writeSample(name + "_max", emptyMap,
      TimeUnit.NANOSECONDS.toMillis(snapShot.getMax))
    writer.writeSample(name + "_median", emptyMap,
      TimeUnit.NANOSECONDS.toMillis(snapShot.getMedian.toLong))
    writer.writeSample(name + "_mean", emptyMap,
      TimeUnit.NANOSECONDS.toMillis(snapShot.getMean.toLong))
    writer.writeSample(name + "_stddev", emptyMap,
      TimeUnit.NANOSECONDS.toMillis(snapShot.getStdDev.toLong))
    writer.writeSample(name + "_count", emptyMap, count)
  }

  @throws[IOException]
  private def writeMetered(dropwizardName: String, metered: Metered): Unit = {
    val name = sanitizeMetricName(dropwizardName)
    writer.writeSample(name, Map("rate" -> "m1"), metered.getOneMinuteRate)
    writer.writeSample(name, Map("rate" -> "m5"), metered.getFiveMinuteRate)
    writer.writeSample(name, Map("rate" -> "m15"), metered.getFifteenMinuteRate)
    writer.writeSample(name, Map("rate" -> "mean"), metered.getMeanRate)
  }

  private def emptyMap: Map[String, String] = Map[String, String]()

  private def getHelpMessage(metricName: String, metric: Metric): String =
    String.format("Generated from Dropwizard metric import (metric=%s, type=%s)",
      metricName, metric.getClass.getName)

  private[prometheus] def sanitizeMetricName(dropwizardName: String): String =
    dropwizardName.replaceAll("[^a-zA-Z0-9:_]", "_")
}

case class MetricType (val text: String)
object MetricTypes {
  val GaugeType = MetricType("gauge")
  val CounterType = MetricType("counter")
  val SummaryType = MetricType("summary")
}
