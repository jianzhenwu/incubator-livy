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

package com.shopee.livy.utils

import scala.collection.mutable

import org.scalatest.FunSuite

class SparkConfUtilsSuite extends FunSuite {

  private val pathConfKeyArray: Array[String] = Array(
    "spark.driver.extraClassPath",
    "spark.driver.extraLibraryPath",
    "spark.executor.extraClassPath",
    "spark.executor.extraLibraryPath")

  private val javaOptionsConfKeyArray: Array[String] = Array(
    "spark.driver.defaultJavaOptions",
    "spark.driver.extraJavaOptions",
    "spark.executor.defaultJavaOptions",
    "spark.executor.extraJavaOptions")


  test("should merge defaultValue when has specified key") {
    for (c <- pathConfKeyArray) {
      val defaultsConf: mutable.Map[String, String] = new mutable.HashMap[String, String]
      defaultsConf.put(c, "/default")
      val userConf: mutable.Map[String, String] = new mutable.HashMap[String, String]
      var mergeConf: mutable.Map[String, String] =
        SparkConfUtils.mergeSparkConf(userConf, defaultsConf)
      assert(mergeConf(c) == "/default")
      userConf.put(c, "/user")
      mergeConf = SparkConfUtils.mergeSparkConf(userConf, defaultsConf)
      assert(mergeConf(c) == "/default:/user")
    }
    for (c <- javaOptionsConfKeyArray) {
      val defaultsConf: mutable.Map[String, String] = new mutable.HashMap[String, String]
      defaultsConf.put(c, "-Ddefault")
      val userConf: mutable.Map[String, String] = new mutable.HashMap[String, String]
      var mergeConf: mutable.Map[String, String] =
        SparkConfUtils.mergeSparkConf(userConf, defaultsConf)
      assert(mergeConf(c) == "-Ddefault")
      userConf.put(c, "-Duser")
      mergeConf = SparkConfUtils.mergeSparkConf(userConf, defaultsConf)
      assert(mergeConf(c) == "-Ddefault -Duser")
    }
  }

}
