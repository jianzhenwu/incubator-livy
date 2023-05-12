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

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, LivyConf, Logging}

/**
 * The HudiConfProcessor should be behind the DefaultsConfSparkProcessor.
 * Because the hudi jar should be merged into aux jar which is included in
 * spark-defaults.conf under spark conf.
 */
object HudiConfProcessor {
  val SPARK_LIVY_HUDI_JAR = "spark.livy.hudi.jar"
  val SPARK_AUX_JAR = "spark.aux.jar"
  val SPARK_SQL_EXTENSIONS = "spark.sql.extensions"

  val SPARK_HUDI_EXTENSION_CLASS_NAME = "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
  val SPARK_HUDI_CATALOG_CLASS_NAME = "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
  val SPARK_HUDI_CATALOG_MIN_VESION = 3.2
}

class HudiConfProcessor extends ApplicationEnvProcessor with Logging {

  import HudiConfProcessor._

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    val appConf = applicationEnvContext.appConf
    val sparkVersion = appConf.get(LivyConf.SPARK_FEATURE_VERSION)

    Option(appConf.get(SPARK_SQL_EXTENSIONS))
      .map(_.split(","))
      .foreach(extension => {
        if (sparkVersion.toDouble >= SPARK_HUDI_CATALOG_MIN_VESION &&
            extension.contains(SPARK_HUDI_EXTENSION_CLASS_NAME)) {
          appConf.put("spark.sql.catalog.spark_catalog", SPARK_HUDI_CATALOG_CLASS_NAME)
        }
      })

    if (!appConf.containsKey(SPARK_LIVY_HUDI_JAR)) {
      return
    }

    val jars = new ArrayBuffer[String]()
    Option(appConf.get(SPARK_AUX_JAR))
      .getOrElse("")
      .split(",")
      .foreach(jar => {
        if (!jar.contains("spark-hudi-bundle")) {
          jars.append(jar)
        }
      })

    jars.append(appConf.getOrDefault(SPARK_LIVY_HUDI_JAR, ""))

    appConf.put(SPARK_AUX_JAR, jars.filter(_.nonEmpty).mkString(","))
  }

}
