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

object SparkConfUtils {

  def mergeSparkConf(conf: mutable.Map[String, String],
      defaultConf: mutable.Map[String, String]): mutable.Map[String, String] = {

    val mergeConf = new mutable.HashMap[String, String]()
    mergeConf ++= conf

    defaultConf.foreach(kv => {
      val key = kv._1
      val defaultValue = kv._2
      if (!conf.contains(key)) {
        mergeConf.put(key, defaultValue)
      } else {
        mergeConf.put(key, mergeIntoValue(key, conf(key), defaultValue))
      }
    })
    mergeConf
  }

  def mergeIntoValue(key: String, value: String, defaultValue: String): String = {
    var separator: String = null
    key match {
      case "spark.driver.extraClassPath" |
           "spark.driver.extraLibraryPath" |
           "spark.executor.extraClassPath" |
           "spark.executor.extraLibraryPath" =>
        // We put the default path in the front, the purpose is to prevent users
        // from overwriting these essential configurations.
        // The previous ones have higher priority.
        separator = ":"

      case "spark.driver.defaultJavaOptions" |
           "spark.driver.extraJavaOptions" |
           "spark.executor.defaultJavaOptions" |
           "spark.executor.extraJavaOptions" =>
        // We put the default option in the front, the purpose is to make the
        // user's configuration take effect first.
        // The latter have higher priority.
        separator = " "

      case _ =>
        return value
    }
    if (null != defaultValue) defaultValue + separator + value
    else value
  }
}
