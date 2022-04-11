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

package com.shopee.livy

import java.util

import scala.collection.JavaConverters.mutableMapAsJavaMapConverter
import scala.collection.mutable

import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import org.apache.livy.ApplicationEnvContext

class SparkConfMappingProcessorSpec extends ScalatraSuite
  with FunSpecLike {

  describe("SparkConfMappingProcessor") {
    it("should not contain conf when user's conf is empty") {
      val appConf = mutable.HashMap[String, String]()
      val context = ApplicationEnvContext(new util.HashMap[String, String](),
        appConf.asJava)
      val processor = new SparkConfMappingProcessor()
      processor.process(context)

      assert(!appConf.contains("spark.pyspark.driver.python"))
      assert(!appConf.contains("spark.driver.memory"))
      assert(!appConf.contains("spark.driver.memoryOverhead"))
      assert(!appConf.contains("spark.executor.memoryOverhead"))
    }

    it("should mapping conf") {
      val appConf = mutable.HashMap[String, String]()
      appConf.put("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "./bin/python")
      appConf.put("spark.yarn.am.memory", "1G")
      appConf.put("spark.driver.memory", "500M")
      appConf.put("spark.yarn.driver.memoryOverhead", "1G")
      appConf.put("spark.driver.memoryOverhead", "500M")
      appConf.put("spark.yarn.executor.memoryOverhead", "1G")

      val context = ApplicationEnvContext(new util.HashMap[String, String](),
        appConf.asJava)
      val processor = new SparkConfMappingProcessor()
      processor.process(context)
      assert(appConf("spark.pyspark.driver.python") == "./bin/python")
      assert(appConf("spark.driver.memory") == "1024M")
      assert(appConf("spark.driver.memoryOverhead") == "1024M")
      assert(appConf("spark.executor.memoryOverhead") == "1G")
    }

    it("should not mapping conf") {
      val appConf = mutable.HashMap[String, String]()
      appConf.put("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "./bin/python")
      appConf.put("spark.pyspark.driver.python", "venv/bin/python")
      appConf.put("spark.yarn.am.memory", "1G")
      appConf.put("spark.driver.memory", "2G")
      appConf.put("spark.yarn.driver.memoryOverhead", "1G")
      appConf.put("spark.driver.memoryOverhead", "2G")
      appConf.put("spark.yarn.executor.memoryOverhead", "1G")
      appConf.put("spark.executor.memoryOverhead", "2G")

      val context = ApplicationEnvContext(new util.HashMap[String, String](),
        appConf.asJava)
      val processor = new SparkConfMappingProcessor()
      processor.process(context)
      assert(appConf("spark.pyspark.driver.python") == "venv/bin/python")
      assert(appConf("spark.driver.memory") == "2048M")
      assert(appConf("spark.driver.memoryOverhead") == "2048M")
      assert(appConf("spark.executor.memoryOverhead") == "2G")
    }
  }

}
