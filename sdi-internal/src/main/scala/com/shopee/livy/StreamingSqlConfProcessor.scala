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

import com.shopee.livy.StreamingSqlConfProcessor.{SPARK_LIVY_STREAMING_SQL_ENABLED, SPARK_SQL_EXTENSIONS, STREAMING_SQL_EXTENSION}

import org.apache.livy.ApplicationEnvContext
import org.apache.livy.ApplicationEnvProcessor


object StreamingSqlConfProcessor {

  val SPARK_LIVY_STREAMING_SQL_ENABLED = "spark.livy.streaming.sql.enabled"
  val SPARK_SQL_EXTENSIONS = "spark.sql.extensions"
  val STREAMING_SQL_EXTENSION = "com.shopee.di.streaming.sql.StreamingSqlExtension"
}

class StreamingSqlConfProcessor extends ApplicationEnvProcessor {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    val appConf = applicationEnvContext.appConf

    if ("true".equalsIgnoreCase(appConf.getOrDefault(SPARK_LIVY_STREAMING_SQL_ENABLED, "false"))) {
      val extensions = new ArrayBuffer[String]()
      Option(appConf.get(SPARK_SQL_EXTENSIONS)).foreach(extensions.append(_))
      extensions.append(STREAMING_SQL_EXTENSION)
      appConf.put(SPARK_SQL_EXTENSIONS, extensions.mkString(","))
      appConf.putIfAbsent(StreamingConfProcessor.SPARK_LIVY_APPLICATION_IS_STREAING, "true")
    }
  }
}
