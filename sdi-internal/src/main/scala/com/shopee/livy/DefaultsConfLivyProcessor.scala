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

import scala.collection.JavaConverters.{mapAsScalaMapConverter, propertiesAsScalaMapConverter}
import scala.collection.mutable

import com.shopee.livy.utils.SparkConfUtils

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, ClassLoaderUtils, Logging}

class DefaultsConfLivyProcessor extends ApplicationEnvProcessor with Logging {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    val appConf = applicationEnvContext.appConf
    val sparkDefaults =
      ClassLoaderUtils.loadAsPropertiesFromClasspath("spark-defaults.conf")
    if (null != sparkDefaults) {
      val sparkDefaultsConf = new mutable.HashMap[String, String]()
      sparkDefaults.asScala.foreach(kv => {
        sparkDefaultsConf.put(kv._1, kv._2)
      })
      val mergeConf = SparkConfUtils.mergeSparkConf(appConf.asScala, sparkDefaultsConf)
      mergeConf.foreach(kv => {
        appConf.put(kv._1, kv._2)
      })
    }
  }

}
