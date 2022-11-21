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

package org.apache.livy.toolkit.metrics

import java.util.concurrent.ConcurrentHashMap

import org.apache.livy.toolkit.metrics.common.{Metrics, MetricsKey, MetricsVariable}
import org.apache.livy.toolkit.metrics.prometheus.PushGateway

trait MetricsInitializer {

  @volatile
  var pushGateway: PushGateway = _
  protected val gauges: ConcurrentHashMap[MetricsKey, Double] =
    new ConcurrentHashMap[MetricsKey, Double]

  def getOrCreatePushGateway(): PushGateway = {
    if (pushGateway == null) {
      this.synchronized {
        if (pushGateway == null) {
          pushGateway = createPushGateWay()
        }
      }
    }
    pushGateway
  }

  private def createPushGateWay(): PushGateway = {
    new PushGateway()
  }

//  def initialize(): Unit = {
//    Metrics.init(getOrCreatePushGateway())
//    registerAllGauge()
//  }

  def registerAllGauge(): Unit

  def registerGauge(key: MetricsKey, value: Double): Unit = {
    if (!gauges.containsKey(key)) {
      gauges.put(key, value)
      Metrics().addGauge(key, new MetricsVariable[Double] {
        override def getValue: Double = gauges.get(key)
      })
    }
  }
}
