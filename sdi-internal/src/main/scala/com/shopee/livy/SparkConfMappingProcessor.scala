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

class SparkConfMappingProcessor extends ApplicationEnvProcessor with Logging {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {

    val appConf = applicationEnvContext.appConf

    Option(appConf.get("spark.yarn.appMasterEnv.PYSPARK_PYTHON"))
      .foreach(e => appConf.putIfAbsent("spark.pyspark.driver.python", e))

    val driverMemory = Option(appConf.get("spark.driver.memory"))
      .map(ByteUtils.byteStringAs(_, ByteUnit.MiB))
      .getOrElse(0L)
    val amMemory = Option(appConf.get("spark.yarn.am.memory"))
      .map(ByteUtils.byteStringAs(_, ByteUnit.MiB))
      .getOrElse(0L)
    Some(Math.max(driverMemory, amMemory))
      .filter(_ > 0)
      .foreach(e => appConf.put("spark.driver.memory", e.toString + "M"))


    val yarnDriverMemoryOverhead = Option(appConf.get("spark.yarn.driver.memoryOverhead"))
      .map(ByteUtils.byteStringAs(_, ByteUnit.MiB))
      .getOrElse(0L)
    val driverMemoryOverhead = Option(appConf.get("spark.driver.memoryOverhead"))
      .map(ByteUtils.byteStringAs(_, ByteUnit.MiB))
      .getOrElse(0L)
    Some(Math.max(driverMemoryOverhead, yarnDriverMemoryOverhead))
      .filter(_ > 0)
      .foreach(e => appConf.put("spark.driver.memoryOverhead", e.toString + "M"))

    Option(appConf.get("spark.yarn.executor.memoryOverhead"))
      .foreach(e => appConf.putIfAbsent("spark.executor.memoryOverhead", e))

  }

}
