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

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, ByteUnit, ByteUtils, Logging}

/**
 * Do not overwrite the user's configuration.
 * Only optimize configurations which with default values.
 */
class SparkResourceOptimizationProcessor extends ApplicationEnvProcessor with Logging {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    val appConf = applicationEnvContext.appConf
    val executorCores = Option(appConf.get("spark.executor.cores"))
      .map(_.trim)
      .getOrElse("1")
    appConf.putIfAbsent("spark.executor.cores", executorCores)
    val executorCoresNum = executorCores.trim.toInt

    var parallelism = 0

    if ("true".equalsIgnoreCase(
      Option(appConf.get("spark.dynamicAllocation.enabled")).getOrElse("").trim)) {
      val maxExecutorsNum =
        Option(appConf.get("spark.dynamicAllocation.maxExecutors"))
          .getOrElse("100").trim.toInt
      val initialExecutorsNum =
        Option(appConf.get("spark.dynamicAllocation.initialExecutors"))
          .getOrElse("0").trim.toInt
      val minExecutorsNum =
        Option(appConf.get("spark.dynamicAllocation.minExecutors"))
          .getOrElse("0").trim.toInt
      val instancesNum = Option(appConf.get("spark.executor.instances"))
        .getOrElse("0").trim.toInt
      val maxExecutors = Math.max(Math.max(Math.max(maxExecutorsNum, initialExecutorsNum),
        minExecutorsNum), instancesNum).toString
      appConf.putIfAbsent("spark.dynamicAllocation.maxExecutors", maxExecutors)
      parallelism = executorCoresNum * maxExecutors.trim.toInt
    } else {
      val executorInstances = Option(appConf.get("spark.executor.instances"))
        .map(_.trim)
        .filter(!_.equals("0"))
        .getOrElse("50")
      // If dynamic is false, the executors should not be 0 even if the session
      // is interactive.
      appConf.put("spark.executor.instances", executorInstances)
      parallelism = executorCoresNum * executorInstances.toInt * 2
    }

    appConf.putIfAbsent("spark.sql.shuffle.partitions", parallelism.toString)
    appConf.putIfAbsent("spark.default.parallelism", parallelism.toString)

    val executorMemory = Option(appConf.get("spark.executor.memory"))
      .getOrElse((4 * executorCoresNum).toString + "G")
    appConf.putIfAbsent("spark.executor.memory", executorMemory)

    val executorMemoryNum = ByteUtils.byteStringAs(executorMemory, ByteUnit.MiB)

    val executorMemoryOverhead = Option(appConf.get("spark.executor.memoryOverhead"))
      .getOrElse(Math.max(executorMemoryNum * 0.25, 1024).toInt.toString + "M")
    appConf.putIfAbsent("spark.executor.memoryOverhead", executorMemoryOverhead)

    val executorExtraJavaOptions = appConf.get("spark.executor.extraJavaOptions")
    if (executorExtraJavaOptions != null) {
      if (!executorExtraJavaOptions.contains("-XX:MaxDirectMemorySize")) {
        appConf.put("spark.executor.extraJavaOptions",
          executorExtraJavaOptions ++ s" -XX:MaxDirectMemorySize=$executorMemoryOverhead")
      }
    } else {
      appConf.put("spark.executor.extraJavaOptions",
        s"-XX:MaxDirectMemorySize=$executorMemoryOverhead")
    }

    val driverMemoryOverhead = Option(appConf.get("spark.driver.memoryOverhead"))
      .getOrElse("1G")
    appConf.putIfAbsent("spark.driver.memoryOverhead", driverMemoryOverhead)

    val driverExtraJavaOptions = appConf.get("spark.driver.extraJavaOptions")
    if (driverExtraJavaOptions != null) {
      if (!driverExtraJavaOptions.contains("-XX:MaxDirectMemorySize")) {
        appConf.put("spark.driver.extraJavaOptions",
          driverExtraJavaOptions ++ s" -XX:MaxDirectMemorySize=$driverMemoryOverhead")
      }
    } else {
      appConf.put("spark.driver.extraJavaOptions",
        s"-XX:MaxDirectMemorySize=$driverMemoryOverhead")
    }
  }
}
