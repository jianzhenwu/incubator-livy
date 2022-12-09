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

  import SparkConfMappingProcessor._

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {

    val appConf = applicationEnvContext.appConf
    // Substitute all deprecated keys to current available config keys
    substitute(appConf)

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

  /**
   * Remove deprecated keys and replace them with the currently available config keys.
   */
  private def substitute(appConf: java.util.Map[String, String]): Unit = {
    configsWithAlternatives.keys.foreach { key =>
      if (appConf.containsKey(key)) {
        warn(s"The configuration key '$key' has been deprecated and " +
          s"may be removed in the future. Please use the new key " +
          s"'${configsWithAlternatives(key)}' instead.")
        appConf.putIfAbsent(configsWithAlternatives(key), appConf.get(key))
        appConf.remove(key)
      }
    }
  }
}

object SparkConfMappingProcessor {

  /**
   * Maps deprecated keys to current available config keys that were used of livy processor.
   */
  private val configsWithAlternatives = Map[String, String](
    "spark.docker.enabled" -> DockerEnvProcessor.SPARK_LIVY_DOCKER_ENABLED,
    "spark.docker.image" -> DockerEnvProcessor.SPARK_LIVY_DOCKER_IMAGE,
    "spark.rss.enabled" -> RssEnvProcessor.SPARK_LIVY_RSS_ENABLED,
    "spark.s3a.enabled" -> S3aEnvProcessor.SPARK_LIVY_S3A_ENABLED,
    "spark.ipynb.env.enabled" -> IpynbEnvProcessor.SPARK_LIVY_IPYNB_ENV_ENABLED,
    "spark.sql.catalog.hbase.enabled" -> "spark.livy.sql.catalog.hbase.enabled",
    "spark.sql.catalog.jdbc.enabled" -> "spark.livy.sql.catalog.jdbc.enabled",
    "spark.sql.catalog.kafka.enabled" -> "spark.livy.sql.catalog.kafka.enabled",
    "spark.sql.catalog.tfrecord.enabled" -> "spark.livy.sql.catalog.tfrecord.enabled",
    "spark.batch.metrics.push.enabled" ->
      BatchMetricProcessor.BATCH_LIVY_METRIC_ENABLED,
    "spark.streaming.metrics.push.enabled" ->
      StreamingMetricProcessor.STEAMING_LIVY_METRIC_ENABLED,
    "spark.structured.streaming.metrics.push.enabled" ->
      StreamingMetricProcessor.STRUCTURED_LIVY_METRIC_ENABLED
  )
}
