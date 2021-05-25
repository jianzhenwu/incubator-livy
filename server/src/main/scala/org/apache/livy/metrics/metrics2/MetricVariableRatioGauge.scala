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

import com.codahale.metrics.RatioGauge
import com.codahale.metrics.RatioGauge.Ratio

import org.apache.livy.metrics.common.MetricsVariable

class MetricVariableRatioGauge extends RatioGauge {

  private var numerator: MetricsVariable[Integer] = _
  private var denominator: MetricsVariable[Integer] = _

  def this(numerator: MetricsVariable[Integer], denominator: MetricsVariable[Integer]) {
    this()
    this.numerator = numerator
    this.denominator = denominator
  }

  override protected def getRatio: RatioGauge.Ratio = {
    val numValue = numerator.getValue
    val denomValue = denominator.getValue
    if (numValue != null && denomValue != null) {
      return Ratio.of(numValue.doubleValue, denomValue.doubleValue)
    }
    Ratio.of(0d, 0d)
  }
}
