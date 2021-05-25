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


package org.apache.livy.metrics.metrics2

import java.io.Closeable
import java.lang.management.ManagementFactory
import java.lang.reflect.{Constructor, InvocationTargetException}
import java.util
import java.util.concurrent.{ConcurrentHashMap, ExecutionException, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.codahale.metrics.{Counter, ExponentiallyDecayingReservoir, Gauge, Histogram, JvmAttributeGaugeSet, Meter, MetricRegistry, MetricSet, Timer}
import com.codahale.metrics.Timer.Context
import com.codahale.metrics.jvm.{BufferPoolMetricSet, ClassLoadingGaugeSet, GarbageCollectorMetricSet, MemoryUsageGaugeSet, ThreadStatesGaugeSet}
import com.google.common.base.{Preconditions, Splitter}
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.collect.Lists
import org.scalatra.metrics.MetricsRegistries

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.metrics.common.{Metrics, MetricsKey, MetricsScope, MetricsVariable}

class CodahaleMetrics(livyConf: LivyConf) extends Metrics with Logging {

  val metricRegistry: MetricRegistry = MetricsRegistries.metricRegistry
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
  val meters: LoadingCache[String, Meter] = CacheBuilder.newBuilder.build(
    new CacheLoader[String, Meter]() {
      override def load(key: String): Meter = {
        val meter = new Meter
        metricRegistry.register(key, meter)
        meter
      }
    })
  val histograms: LoadingCache[String, Histogram] = CacheBuilder.newBuilder.build(
    new CacheLoader[String, Histogram]() {
      override def load(key: String): Histogram = {
        val histogram = metricRegistry.histogram(key)
        histogram
      }
    })
  val gauges: ConcurrentHashMap[String, Gauge[_]] = new ConcurrentHashMap[String, Gauge[_]]
  private val reporters: mutable.HashSet[Closeable] = new mutable.HashSet[Closeable]

  // register JVM metrics
  registerAll("jvmAttribute", new JvmAttributeGaugeSet)
  registerAll("gc", new GarbageCollectorMetricSet)
  registerAll("buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer))
  registerAll("memory", new MemoryUsageGaugeSet)
  registerAll("threads", new ThreadStatesGaugeSet)
  registerAll("classLoading", new ClassLoadingGaugeSet)

  initReporting()
  private val timersLock = new ReentrantLock()
  private val countersLock = new ReentrantLock()
  private val gaugesLock = new ReentrantLock()
  private val metersLock = new ReentrantLock()
  private val histogramsLock = new ReentrantLock()
  private val threadLocalScopes = new ThreadLocal[mutable.HashMap[String, CodahaleMetricsScope]] {
    override protected def initialValue = new mutable.HashMap[String, CodahaleMetricsScope]
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
    meters.invalidateAll()
  }

  override def startStoredScope(key: MetricsKey): Unit = {
    if (threadLocalScopes.get.contains(key.name)) {
      threadLocalScopes.get.get(key.name).foreach(_.open())
    }
    else {
      threadLocalScopes.get.put(key.name, new CodahaleMetricsScope(key.name))
    }
  }

  override def endStoredScope(key: MetricsKey): Unit = {
    if (threadLocalScopes.get.contains(key.name)) {
      threadLocalScopes.get.get(key.name).foreach(_.close())
      threadLocalScopes.get.remove(key.name)
    }
  }

  override def createScope(key: MetricsKey): CodahaleMetricsScope =
    new CodahaleMetricsScope(key.name)

  override def endScope(scope: MetricsScope): Unit =
    scope.asInstanceOf[CodahaleMetricsScope].close()

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

  override def decrementCounter(key: MetricsKey): java.lang.Long =
    decrementCounter(key, 1L)

  override def decrementCounter(key: MetricsKey, decrement: java.lang.Long): java.lang.Long = {
    val name = key.name
    try {
      countersLock.lock()
      counters.get(name).dec(decrement)
      counters.get(name).getCount
    } catch {
      case ee: ExecutionException =>
        throw new IllegalStateException("Error retrieving counter from the metric registry ", ee)
    } finally {
      countersLock.unlock()
    }
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

  override def addRatio(
      key: MetricsKey,
      numerator: MetricsVariable[Integer],
      denominator: MetricsVariable[Integer]): Unit = {

    Preconditions.checkArgument(numerator != null,
      "Numerator must not be null".asInstanceOf[AnyRef])
    Preconditions.checkArgument(denominator != null,
      "Denominator must not be null".asInstanceOf[AnyRef])

    val gauge = new MetricVariableRatioGauge(numerator, denominator)
    addGaugeInternal(key, gauge)
  }

  override def markMeter(key: MetricsKey): Unit = {
    try {
      metersLock.lock()
      val meter = meters.get(key.name)
      meter.mark()
    } catch {
      case e: ExecutionException =>
        throw new IllegalStateException("Error retrieving meter " + key.name
          + " from the metric registry ", e)
    } finally {
      metersLock.unlock()
    }
  }

  override def updateHistogram(key: MetricsKey, value: Integer): Unit = {
    try {
      histogramsLock.lock()
      histograms.get(key.name).update(value)
    } finally {
      histogramsLock.unlock()
    }
  }

  override def updateHistogram(key: MetricsKey, value: java.lang.Long): Unit = {
    try {
      histogramsLock.lock()
      histograms.get(key.name).update(value)
    } finally {
      histogramsLock.unlock()
    }
  }

  private def getTimer(name: String) = {
    val key = name
    try {
      timersLock.lock()
      val timer = timers.get(key)
      timer
    } catch {
      case e: ExecutionException =>
        throw new IllegalStateException("Error retrieving timer " + name +
          " from the metric registry ", e)
    } finally {
      timersLock.unlock()
    }
  }

  /**
   * Initializes reporters
   */
  private def initReporting(): Unit = {
    if (!initCodahaleMetricsReporterClasses()) {
      warn("Unable to initialize metrics reporting")
    }
    if (reporters.isEmpty) {
      warn("No reporters configured for codahale metrics!")
    }
  }

  /**
   * Initializes reporting.
   *
   * @return whether initialization was successful or not
   */
  private def initCodahaleMetricsReporterClasses(): Boolean = {
    val reporterClasses: util.List[String] = Lists.newArrayList(Splitter.on(",").trimResults
      .omitEmptyStrings.split(livyConf.get(LivyConf.CODAHALE_METRICS_REPORTER_CLASSES)))
    if (reporterClasses.isEmpty) return false
    import scala.collection.JavaConverters._
    for (reporterClass <- reporterClasses.asScala) {
      var clz: Class[_] = null
      try clz = Class.forName(reporterClass)
      catch {
        case e: ClassNotFoundException =>
          error("Unable to instantiate metrics reporter class " + reporterClass, e)
          throw new IllegalArgumentException(e)
      }
      try {
        // Note: Hadoop metric reporter does not support tags.
        // We create a single reporter for all metrics.
        val constructor: Constructor[_] =
        clz.getConstructor(classOf[MetricRegistry], classOf[LivyConf])
        val reporter: CodahaleReporter =
          constructor.newInstance(metricRegistry, livyConf).asInstanceOf[CodahaleReporter]
        reporter.start()
        reporters.add(reporter)
      } catch {
        case e@(_: NoSuchMethodException | _: InstantiationException
                | _: IllegalAccessException | _: InvocationTargetException) =>
          error("Unable to instantiate using constructor(MetricRegistry, HiveConf) for"
            + " reporter " + reporterClass, e)
          throw new IllegalArgumentException(e)
      }
    }
    true
  }

  private def registerAll(prefix: String, metricSet: MetricSet): Unit = {
    for (entry <- metricSet.getMetrics.entrySet.asScala) {
      entry.getValue match {
        case metricSet: MetricSet =>
          registerAll(prefix + "." + entry.getKey, metricSet)
        case _ =>
          metricRegistry.register(prefix + "." + entry.getKey, entry.getValue)
      }
    }
  }

  class CodahaleMetricsScope(private val name: String) extends MetricsScope {
    private val timer: Timer = getTimer(name)
    private var timerContext: Context = _
    private var isOpen = false
    open()

    /**
     * Opens scope, and makes note of the time started
     *
     */
    def open(): Unit = {
      if (!isOpen) {
        isOpen = true
        timerContext = timer.time
      }
      else {
        warn("Scope named " + name + " is not closed, cannot be opened.")
      }
    }

    /**
     * Closes scope, and records the time taken
     */
    def close(): Unit = {
      if (isOpen) {
        timerContext.close()
      }
      else {
        warn("Scope named " + name + " is not open, cannot be closed.")
      }
      isOpen = false
    }
  }
}
