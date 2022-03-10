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


package org.apache.livy.toolkit.metrics.metrics2

import java.io.Closeable
import java.util.concurrent.{ConcurrentHashMap, ExecutionException, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable

import com.codahale.metrics.{Counter, ExponentiallyDecayingReservoir, Gauge, MetricRegistry, Timer}
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.spark.SparkEnv

import org.apache.livy.Logging
import org.apache.livy.toolkit.metrics.common.{Metrics, MetricsKey, MetricsVariable}
import org.apache.livy.toolkit.metrics.prometheus.PushGateway
import org.apache.livy.toolkit.metrics.prometheus.reporter.PrometheusReporterBuilder
import org.apache.livy.toolkit.metrics.registry.CodahaleMetricRegistry

class CodahaleMetrics(output: PushGateway) extends Metrics with Logging {
  val metricRegistry: MetricRegistry = CodahaleMetricRegistry.getRegistry()
  private val reporters: mutable.HashSet[Closeable] = new mutable.HashSet[Closeable]

  private val timersLock = new ReentrantLock()
  private val countersLock = new ReentrantLock()
  private val gaugesLock = new ReentrantLock()

  val timers: LoadingCache[String, Timer] = CacheBuilder.newBuilder.build(
    new CacheLoader[String, Timer]() {
      override def load(key: String): Timer = {
        val timer = new Timer(new ExponentiallyDecayingReservoir)
        metricRegistry.register(key, timer)
        timer
      }
    })

  val counters: LoadingCache[String, Counter] = CacheBuilder.newBuilder.build(
    new CacheLoader[String, Counter]() {
      override def load(key: String): Counter = {
        val counter = new Counter
        metricRegistry.register(key, counter)
        counter
      }
    })

  val gauges: ConcurrentHashMap[String, Gauge[_]] = new ConcurrentHashMap[String, Gauge[_]]

  initReporting(output)

  /**
   * Initializes reporters
   */
  private def initReporting(pushGateway: PushGateway): Unit = {
    val reporter = new PrometheusReporterBuilder()
      .setRegistry(CodahaleMetricRegistry.getRegistry())
      .setPushGateway(pushGateway)
      .build()
    val interval = SparkEnv.get.conf.get("spark.streaming.metrics.send.interval").toInt
    reporter.start(interval, TimeUnit.SECONDS)
    reporters.add(reporter)
  }


  override def close(): Unit = {
    if (reporters != null) {
      reporters.foreach(_.close())
    }
    for (metric <- metricRegistry.getMetrics.entrySet.asScala) {
      metricRegistry.remove(metric.getKey)
    }
    timers.invalidateAll()
    counters.invalidateAll()
  }

  override def addGauge(key: MetricsKey, variable: MetricsVariable[_]): Unit = {
    val gauge = new Gauge[Any] {
      override def getValue: Any = variable.getValue
    }
    addGaugeInternal(key, gauge)
  }

  private def addGaugeInternal(key: MetricsKey, gauge: Gauge[_]): Unit = {
    try {
      gaugesLock.lock()
      gauges.put(key.name, gauge)
      // Metrics throws an Exception if we don't do this when the key already exists
      if (metricRegistry.getGauges.containsKey(key.name)) {
        warn("A Gauge with name [" + key.name + "] already exists. " +
        " The old gauge will be overwritten, but this is not recommended")
        metricRegistry.remove(key.name)
      }
      metricRegistry.register(key.name, gauge)
    } finally {
      gaugesLock.unlock()
    }
  }

  override def removeGauge(key: MetricsKey): Unit = {
    try {
      gaugesLock.lock()
      gauges.remove(key.name)
      // Metrics throws an Exception if we don't do this when the key already exists
      if (metricRegistry.getGauges.containsKey(key.name)) metricRegistry.remove(key.name)
    } finally {
      gaugesLock.unlock()
    }
  }

  override def updateTimer(key: MetricsKey, value: java.lang.Long, unit: TimeUnit): Unit = {
    try {
      timersLock.lock()
      timers.get(key.name).update(value, unit)
    } finally {
      timersLock.unlock()
    }
  }

  override def incrementCounter(key: MetricsKey): java.lang.Long =
    incrementCounter(key, 1L)

  override def incrementCounter(key: MetricsKey, increment: java.lang.Long): java.lang.Long = {
    try {
      countersLock.lock()
      counters.get(key.name).inc(increment)
      counters.get(key.name).getCount
    } catch {
      case ee: ExecutionException =>
        throw new IllegalStateException("Error retrieving counter from the metric registry ", ee)
    } finally {
      countersLock.unlock()
    }
  }

  override def decrementCounter(key: MetricsKey): java.lang.Long = {
    decrementCounter(key, 1L)
  }

  override def decrementCounter(key: MetricsKey, decrement: java.lang.Long): java.lang.Long = {
    try {
      countersLock.lock()
      counters.get(key.name).dec(decrement)
      counters.get(key.name).getCount
    } catch {
      case ee: ExecutionException =>
        throw new IllegalStateException("Error retrieving counter from the metric registry ", ee)
    } finally {
      countersLock.unlock()
    }
  }
}

