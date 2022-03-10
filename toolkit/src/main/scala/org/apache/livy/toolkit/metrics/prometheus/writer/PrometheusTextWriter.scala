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

import java.io.{FilterWriter, IOException, Writer}

class PrometheusTextWriter(out : Writer) extends FilterWriter(out) {
  @throws[IOException]
  def writeHelp(name: String, value: String): Unit = {
    write("# HELP ")
    write(name)
    write(' ')
    writeEscapedHelp(value)
    write('\n')
  }

  @throws[IOException]
  def writeType(name: String, metricType: MetricType): Unit = {
    write("# TYPE ")
    write(name)
    write(" ")
    write(metricType.text)
    write('\n')
  }

  @throws[IOException]
  def writeSample(name: String, labels: Map[String, String], value: Double) {
    write(name)
    if (labels.size > 0) {
      write('{')
      labels.foreach(x => {
        write(x._1)
        write("=\"")
        writeEscapedLabelValue(x._2)
        write("\",")
      })
      write('}')
    }
    write(" ")
    write(doubleToGoString(value))
    write('\n')
  }

  @throws[IOException]
  private def writeEscapedHelp(s: String): Unit = {
    for (i <- 0 until s.length) {
      val c: Char = s.charAt(i)
      c match {
        case '\\' => append("\\\\")
        case '\n' => append("\\n")
        case _ => append(c)
      }
    }
  }

  @throws[IOException]
  private def writeEscapedLabelValue(s: String): Unit = {
    for (i <- 0 until s.length) {
      val c: Char = s.charAt(i)
      c match {
        case '\\' => append("\\\\")
        case '\"' => append("\\\"")
        case '\n' => append("\\n")
        case _ => append(c)
      }
    }
  }

  private def doubleToGoString(d: Double): String = {
    if (d == Double.PositiveInfinity) return "+Inf"
    if (d == Double.NegativeInfinity) return "-Inf"
    if (d == Double.NaN) return "NaN"
    d.toString()
  }
}
