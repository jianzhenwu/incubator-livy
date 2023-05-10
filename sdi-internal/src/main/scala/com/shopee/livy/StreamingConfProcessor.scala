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

import java.io.{File, FileNotFoundException}

import com.shopee.livy.StreamingConfProcessor._

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, Logging}

object StreamingConfProcessor {
  val SPARK_LIVY_APPLICATION_IS_STREAING = "spark.livy.isStreaming"
  val CUSTOM_LOG_PROPERTIES_FILE = "spark-streaming-log4j.properties"
}

class StreamingConfProcessor extends ApplicationEnvProcessor with Logging {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    val appConf = applicationEnvContext.appConf

    // If spark.livy.isStreaming is set to true, load custom log4j properties file.
    if ("true".equalsIgnoreCase(
      appConf.getOrDefault(SPARK_LIVY_APPLICATION_IS_STREAING, "false"))) {
      // Add custom log4j properties file to spark conf.
      val log4jPropertiesFile = getLog4jPropertiesFile()
      log4jPropertiesFile match {
        case Some(file) =>
          val sparkFilesOption = appConf.getOrDefault("spark.files", "")
          if (sparkFilesOption.isEmpty) {
            appConf.put("spark.files", file.getCanonicalPath)
          } else {
            appConf.put("spark.files", s"$sparkFilesOption,${file.getCanonicalPath}")
          }
        case _ => throw new FileNotFoundException(s"Custom log4j properties file is not exist.")
      }

      // Add log4j config for the driver.
      val driverExtraJavaOptions = appConf.get("spark.driver.extraJavaOptions")
      if (driverExtraJavaOptions != null) {
        if (!driverExtraJavaOptions.contains("-Dlog4j.configuration")) {
          appConf.put("spark.driver.extraJavaOptions",
            driverExtraJavaOptions ++ s" -Dlog4j.configuration=file:$CUSTOM_LOG_PROPERTIES_FILE")
        }
      } else {
        appConf.put("spark.driver.extraJavaOptions",
          s"-Dlog4j.configuration=file:$CUSTOM_LOG_PROPERTIES_FILE")
      }

      // Add log4j config for executors.
      val executorExtraJavaOptions = appConf.get("spark.executor.extraJavaOptions")
      if (executorExtraJavaOptions != null) {
        if (!executorExtraJavaOptions.contains("-Dlog4j.configuration")) {
          appConf.put("spark.executor.extraJavaOptions",
            executorExtraJavaOptions ++ s" -Dlog4j.configuration=file:$CUSTOM_LOG_PROPERTIES_FILE")
        }
      } else {
        appConf.put("spark.executor.extraJavaOptions",
          s"-Dlog4j.configuration=file:$CUSTOM_LOG_PROPERTIES_FILE")
      }
    }
  }

  private def getLog4jPropertiesFile(): Option[File] = {
    val configDir = sys.env.get("LIVY_CONF_DIR")
      .orElse(sys.env.get("LIVY_HOME").map(path => s"$path${File.separator}conf"))
      .map(new File(_))
      .filter(_.exists())
    configDir.map(new File(_, CUSTOM_LOG_PROPERTIES_FILE)).filter(_.exists())
  }
}

