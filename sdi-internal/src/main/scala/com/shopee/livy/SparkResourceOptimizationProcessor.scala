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

import org.apache.commons.lang3.StringUtils

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, ByteUnit, ByteUtils, Logging}

class SparkResourceOptimizationProcessor extends ApplicationEnvProcessor with Logging {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    val appConf = applicationEnvContext.appConf
    val executorCores = Option(appConf.get("spark.executor.cores"))
      .map(_.trim)
      .getOrElse("1")

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
      appConf.put("spark.dynamicAllocation.maxExecutors", maxExecutors)
      parallelism = executorCores.trim.toInt * maxExecutors.trim.toInt
    } else {
      val executorInstances = Option(appConf.get("spark.executor.instances"))
        .map(_.trim)
        .filter(!_.equals("0"))
        .getOrElse("50")
      appConf.put("spark.executor.instances", executorInstances)
      parallelism = executorCores.trim.toInt * executorInstances.toInt * 2
    }

    appConf.put("spark.sql.shuffle.partitions", parallelism.toString)
    appConf.put("spark.default.parallelism", parallelism.toString)

    val driverMemoryOverhead = appConf.get("spark.driver.memoryOverhead")
    if (StringUtils.isBlank(driverMemoryOverhead)
      || ByteUtils.byteStringAs(driverMemoryOverhead, ByteUnit.MiB) < 1024L) {
      appConf.put("spark.driver.memoryOverhead", "1G")
    }

    val executorMemoryOverhead = appConf.get("spark.executor.memoryOverhead")
    if (StringUtils.isBlank(executorMemoryOverhead)
      || ByteUtils.byteStringAs(executorMemoryOverhead, ByteUnit.MiB) < 1024L) {
      appConf.put("spark.executor.memoryOverhead", "1G")
    }
  }
}
