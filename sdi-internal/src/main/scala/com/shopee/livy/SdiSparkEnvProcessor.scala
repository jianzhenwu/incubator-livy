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

import com.shopee.livy.SdiSparkEnvProcessor.processorInstances

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor}
import org.apache.livy.utils.LivyProcessorException

object SdiSparkEnvProcessor {

  /**
   * Do not change the order of DefaultsConfLivyProcessor and DefaultsConfSparkProcessor
   * in processorNames unless you have a valid reason.
   */
  val processorNames = Seq(
    "com.shopee.livy.SdiProjectConfProcessor",
    "com.shopee.livy.SparkConfMappingProcessor",
    "com.shopee.livy.DefaultsConfLivyProcessor",
    "com.shopee.livy.SdiHadoopEnvProcessor",
    "com.shopee.livy.S3aEnvProcessor",
    "com.shopee.livy.SparkResourceOptimizationProcessor",
    "com.shopee.livy.StreamingMetricProcessor",
    "com.shopee.livy.BatchMetricProcessor",
    "com.shopee.livy.DockerEnvProcessor",
    "com.shopee.livy.RssEnvProcessor",
    "com.shopee.livy.SparkDatasourceProcessor",
    "com.shopee.livy.IpynbEnvProcessor",
    "com.shopee.livy.SdiYarnAmEnvProcessor",
    "com.shopee.livy.DefaultsConfSparkProcessor",
    "com.shopee.livy.HudiConfProcessor"
  )

  lazy val processorInstances: Seq[ApplicationEnvProcessor] =
    processorNames.map(c => {
      ApplicationEnvProcessor.apply(c)
    })
}

class SdiSparkEnvProcessor extends ApplicationEnvProcessor {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    try {
      processorInstances.foreach(_.process(applicationEnvContext))
    } catch {
      case e: Exception =>
        throw new LivyProcessorException(e.getMessage, e.getCause)
    }
  }

}
