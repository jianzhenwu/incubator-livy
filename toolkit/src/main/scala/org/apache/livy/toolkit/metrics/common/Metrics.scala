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


package org.apache.livy.toolkit.metrics.common

import java.util.concurrent.TimeUnit

import org.apache.livy.toolkit.metrics.metrics2.CodahaleMetrics
import org.apache.livy.toolkit.metrics.prometheus.PushGateway

object Metrics {
  @volatile
  private var metrics: Metrics = _

  def init(outPut: PushGateway): Unit = {
    if (metrics == null) {
      this.synchronized {
        if (metrics == null) {
          metrics = new CodahaleMetrics(outPut)
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

trait Metrics {
  /**
   * Deinitializes the Metrics system.
   */
  @throws[Exception]
  def close(): Unit

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
   * Add a timer value of the given name.
   *
   * @param name
   * @param value
   * @param unit
   * @return
   */
  def updateTimer(key: MetricsKey, value: java.lang.Long, unit: TimeUnit): Unit

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
}
