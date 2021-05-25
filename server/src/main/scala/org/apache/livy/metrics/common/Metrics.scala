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

import java.util.concurrent.TimeUnit

import org.apache.livy.LivyConf
import org.apache.livy.metrics.metrics2.CodahaleMetrics

object Metrics {
  @volatile
  private var metrics: Metrics = _

  def init(livyConf: LivyConf): Unit = {
    if (metrics == null) {
      this.synchronized {
        if (metrics == null) {
          metrics = new CodahaleMetrics(livyConf)
        }
      }
    }
  }

  def close(): Unit = {
    this.synchronized {
      if (metrics != null) {
        metrics.close()
        metrics = null
      }
    }
  }

  def apply(): Metrics = {
    if (metrics == null) {
      throw new IllegalStateException("Metrics not initialized yet!")
    } else {
      metrics
    }
  }
}

/**
 * Generic Metics interface.
 */
trait Metrics {
  /**
   * Deinitializes the Metrics system.
   */
  @throws[Exception]
  def close(): Unit

  /**
   *
   * @param name starts a scope of a given name.  Scopes is stored as thread-local variable.
   */
  def startStoredScope(key: MetricsKey): Unit

  /**
   * Closes the stored scope of a given name.
   * Note that this must be called on the same thread as where the scope was started.
   *
   * @param name
   */
  def endStoredScope(key: MetricsKey): Unit

  /**
   * Create scope with given name and returns it.
   *
   * @param name
   * @return
   */
  def createScope(key: MetricsKey): MetricsScope

  /**
   * Close the given scope.
   *
   * @param scope
   */
  def endScope(scope: MetricsScope): Unit

  /**
   * Add a timer value of the given name.
   *
   * @param name
   * @param value
   * @param unit
   * @return
   */
  def updateTimer(key: MetricsKey, value: java.lang.Long, unit: TimeUnit): Unit

  // Counter-related methods

  /**
   * Increments a counter of the given name by 1.
   *
   * @param name
   * @return
   */
  def incrementCounter(key: MetricsKey): java.lang.Long

  /**
   * Increments a counter of the given name by "increment"
   *
   * @param name
   * @param increment
   * @return
   */
  def incrementCounter(key: MetricsKey, increment: java.lang.Long): java.lang.Long


  /**
   * Decrements a counter of the given name by 1.
   *
   * @param name
   * @return
   */
  def decrementCounter(key: MetricsKey): java.lang.Long

  /**
   * Decrements a counter of the given name by "decrement"
   *
   * @param name
   * @param decrement
   * @return
   */
  def decrementCounter(key: MetricsKey, decrement: java.lang.Long): java.lang.Long


  /**
   * Adds a metrics-gauge to track variable.  For example, number of open database connections.
   *
   * @param name     name of gauge
   * @param variable variable to track.
   */
  def addGauge(key: MetricsKey, variable: MetricsVariable[_]): Unit


  /**
   * Removed the gauge added by addGauge.
   *
   * @param name name of gauge
   */
  def removeGauge(key: MetricsKey): Unit


  /**
   * Add a ratio metric to track the correlation between two variables
   *
   * @param name        name of the ratio gauge
   * @param numerator   numerator of the ratio
   * @param denominator denominator of the ratio
   */
  def addRatio(key: MetricsKey, numerator: MetricsVariable[Integer],
               denominator: MetricsVariable[Integer]): Unit

  /**
   * Mark an event occurance for a meter. Meters measure the rate of an event and track
   * 1/5/15 minute moving averages
   *
   * @param name name of the meter
   */
  def markMeter(key: MetricsKey): Unit

  /**
   * Add a histogram value of the given name.
   *
   * @param name
   * @param value
   * @return
   */
  def updateHistogram(key: MetricsKey, value: Integer): Unit

  /**
   * Add a histogram value of the given name.
   *
   * @param name
   * @param value
   * @return
   */
  def updateHistogram(key: MetricsKey, value: java.lang.Long): Unit
}
