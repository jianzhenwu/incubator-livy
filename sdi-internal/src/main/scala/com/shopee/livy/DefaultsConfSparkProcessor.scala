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

import java.io.File

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable

import com.shopee.livy.utils.SparkConfUtils

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, Logging, SessionType, Utils}
import org.apache.livy.client.common.ClientConf

class DefaultsConfSparkProcessor extends ApplicationEnvProcessor with Logging {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {

    val appConf = applicationEnvContext.appConf
    val sparkConfDir = appConf.asScala.get(ClientConf.LIVY_APPLICATION_SPARK_CONF_DIR_KEY)
      .orElse(applicationEnvContext.env.asScala.get("SPARK_CONF_DIR"))

    if (applicationEnvContext.sessionType.isEmpty) {
      throw new ProcessorException(
        "SessionType can not be empty when using DefaultsConfSparkProcessor.")
    }
    sparkConfDir.foreach(dir => {
      val sparkDefaults = new File(dir + File.separator + "spark-defaults.conf")
      if (sparkDefaults.isFile) {
        val sparkDefaultsConf = new mutable.HashMap[String, String]()
        val props = Utils.getPropertiesFromFile(sparkDefaults)
        props.foreach(kv => {
          // Contains configuration that needs to be merged into appConf when
          // using batches in order to reduce the length of submitting command.
          if (applicationEnvContext.sessionType.get == SessionType.Batches) {
            if (appConf.containsKey(kv._1)) {
              sparkDefaultsConf.put(kv._1, kv._2)
            }
          } else {
            sparkDefaultsConf.put(kv._1, kv._2)
          }
        })
        val mergeConf = SparkConfUtils.mergeSparkConf(appConf.asScala, sparkDefaultsConf)
        mergeConf.foreach(kv => {
          appConf.put(kv._1, kv._2)
        })
      } else {
        logger.error(s"Fail to load ${sparkDefaults.getAbsolutePath}")
      }
    })
  }
}
