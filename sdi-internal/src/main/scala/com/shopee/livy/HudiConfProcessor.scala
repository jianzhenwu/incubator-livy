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

import scala.collection.mutable.ArrayBuffer

import com.shopee.livy.HudiConfProcessor.{SPARK_AUX_JAR, SPARK_LIVY_HUDI_JAR}

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, Logging}

object HudiConfProcessor {
  val SPARK_LIVY_HUDI_JAR = "spark.livy.hudi.jar"
  val SPARK_AUX_JAR = "spark.aux.jar"
}

class HudiConfProcessor extends ApplicationEnvProcessor with Logging {
  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    val appConf = applicationEnvContext.appConf

    val jars = new ArrayBuffer[String]()
    Option(appConf.get(SPARK_LIVY_HUDI_JAR))
      .filter(_.nonEmpty)
      .foreach(jars += _.trim)
    Option(appConf.get(SPARK_AUX_JAR))
      .filter(_.nonEmpty)
      .foreach(jars += _.trim)
    if (jars.nonEmpty) {
      appConf.put(SPARK_AUX_JAR, jars.mkString(","))
    }
  }
}
