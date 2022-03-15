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

package com.shopee.livy.auth

import java.util

import scala.collection.JavaConverters.mutableMapAsJavaMapConverter
import scala.collection.mutable

import com.shopee.livy.SparkResourceOptimizationProcessor
import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import org.apache.livy.ApplicationEnvContext

class SparkResourceOptimizationProcessorSpec extends ScalatraSuite
  with FunSpecLike {

  describe("SparkResourceOptimizationProcessor") {
    it("should set default value when dynamicAllocation is false") {

      val appConf = mutable.HashMap[String, String](
        "spark.executor.cores" -> "1",
        "spark.dynamicAllocation.enabled" -> "false")
      val context = ApplicationEnvContext(new util.HashMap[String, String](),
        appConf.asJava)
      val processor = new SparkResourceOptimizationProcessor()
      processor.process(context)

      assert(appConf("spark.executor.cores") == "1")
      assert(appConf("spark.dynamicAllocation.enabled") == "false")
      assert(appConf("spark.executor.instances") == "50")
      assert(appConf("spark.sql.shuffle.partitions").toInt == 100)
      assert(appConf("spark.default.parallelism").toInt == 100)
      assert(appConf("spark.driver.memoryOverhead") == "1G")
      assert(appConf("spark.executor.memoryOverhead") == "1G")
    }
  }

  it("should set default value when dynamicAllocation is true") {
    val appConf = mutable.HashMap[String, String](
      "spark.executor.cores" -> "1",
      "spark.dynamicAllocation.enabled" -> "true")
    val context = ApplicationEnvContext(new util.HashMap[String, String](),
      appConf.asJava)
    val processor = new SparkResourceOptimizationProcessor()
    processor.process(context)

    assert(appConf("spark.executor.cores") == "1")
    assert(appConf("spark.dynamicAllocation.enabled") == "true")
    assert(appConf("spark.dynamicAllocation.maxExecutors") == "100")
    assert(appConf("spark.sql.shuffle.partitions").toInt == 100)
    assert(appConf("spark.default.parallelism").toInt == 100)
    assert(appConf("spark.driver.memoryOverhead") == "1G")
    assert(appConf("spark.executor.memoryOverhead") == "1G")
  }

}
