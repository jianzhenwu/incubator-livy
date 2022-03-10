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

import java.io.StringWriter

import org.scalatest.FunSuite

import org.apache.livy.toolkit.metrics.prometheus.writer.{MetricTypes, PrometheusTextWriter}

class PrometheusTextWriterSuite extends FunSuite {
  test("test write help info") {
    val buffer = new StringWriter
    val writer = new PrometheusTextWriter(buffer)
    writer.writeHelp("lorem.ipsum", "A\nB\nC")
    assert(buffer.toString.equals("# HELP lorem.ipsum A\\nB\\nC\n"))
  }

  test("test write sample info") {
    val buffer = new StringWriter
    val writer = new PrometheusTextWriter(buffer)
    writer.writeSample("lorem.ipsum", Map[String, String](), 1.0D)
    assert(buffer.toString.equals("lorem.ipsum 1.0\n"))
  }

  test("test write labelized sample info") {
    val buffer = new StringWriter
    val writer = new PrometheusTextWriter(buffer)
    writer.writeSample("lorem.ipsum", simpleMap, 2.0D)
    assert(buffer.toString.equals("lorem.ipsum{quantile=\"1.0\",} 2.0\n"))
  }

  test("test write labelized sample info2") {
    val buffer = new StringWriter
    val writer = new PrometheusTextWriter(buffer)
    writer.writeSample("lorem.ipsum", doubleMap, 2.0D)
    print(buffer.toString)
    assert(buffer.toString.equals("lorem.ipsum{quantile=\"1.0\",centile=\"3.0\",} 2.0\n"))
  }

  private def simpleMap: Map[String, String] = {
    var map = Map("quantile" -> "1.0")
    map
  }

  private def doubleMap: Map[String, String] = {
    var map = simpleMap
    map += ("centile" -> "3.0")
    map
  }
}
