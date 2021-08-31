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

package org.apache.livy.utils

import org.scalatest.FunSpec

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.server.batch.CreateBatchRequest
import org.apache.livy.sessions.Session.prepareConf

class SparkProcessBuilderSpec extends FunSpec with LivyBaseUnitTestSuite {

  val SPARK_DEFAULT_CONFIG_KEY = "spark.test.conf"
  val SPARK_DEFAULT_CONFIG_VALUE = "true"

  describe("SparkProcessBuilder") {
    val livyConf = new LivyConf()

    it("should read spark-default.conf from classpath") {
      val processBuilder = new SparkProcessBuilder(livyConf, None)
      assert(
        processBuilder.conf(SPARK_DEFAULT_CONFIG_KEY).orNull == SPARK_DEFAULT_CONFIG_VALUE,
        s"Config $SPARK_DEFAULT_CONFIG_KEY is not $SPARK_DEFAULT_CONFIG_VALUE")
    }

    it("should take user config first") {
      val req = new CreateBatchRequest()
      req.conf = Map(SPARK_DEFAULT_CONFIG_KEY -> "false")
      val conf = SparkApp.prepareSparkConf(
        "livy-test",
        livyConf,
        prepareConf(
          req.conf, req.jars, req.files, req.archives, req.pyFiles, livyConf))
      val processBuilder = new SparkProcessBuilder(livyConf, None)
      processBuilder.conf(conf)
      assert(processBuilder.conf(SPARK_DEFAULT_CONFIG_KEY).orNull == "false",
        s"Config $SPARK_DEFAULT_CONFIG_KEY should be false")
    }
  }
}
