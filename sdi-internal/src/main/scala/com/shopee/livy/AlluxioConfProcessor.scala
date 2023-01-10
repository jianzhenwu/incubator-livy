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

import com.shopee.livy.AlluxioConfProcessor.{ALLUXIO_FUSE_JAVA_OPTS, ALLUXIO_JAVA_OPTS, SPARK_LIVY_ALLUXIO_ARCHIVE, SPARK_LIVY_ALLUXIO_ENV_ENABLED, SPARK_LIVY_ALLUXIO_HOME}

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, Logging}

object AlluxioConfProcessor {
  val SPARK_LIVY_ALLUXIO_ENV_ENABLED = "spark.livy.alluxio.enabled"
  val SPARK_LIVY_ALLUXIO_ARCHIVE = "spark.livy.alluxio.archive"
  val SPARK_LIVY_ALLUXIO_HOME = "spark.livy.alluxio.home"
  val ALLUXIO_FUSE_JAVA_OPTS = "spark.livy.alluxio.fuse.java.opts"
  val ALLUXIO_JAVA_OPTS = "spark.livy.alluxio.java.opts"
}

class AlluxioConfProcessor extends ApplicationEnvProcessor with Logging {
  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    val appConf = applicationEnvContext.appConf

    val envEnabled = appConf.getOrDefault(SPARK_LIVY_ALLUXIO_ENV_ENABLED, "false")

    if ("false".equalsIgnoreCase(envEnabled)) {
      return
    }

    val archives = new ArrayBuffer[String]()
    Option(appConf.get("spark.archives")).getOrElse("").split(",").foreach(archive => {
      archives.append(archive)
    })

    val alluxioArchive = appConf.get(SPARK_LIVY_ALLUXIO_ARCHIVE)
    if (alluxioArchive != null && alluxioArchive.nonEmpty) {
      archives.append(alluxioArchive)
      appConf.put("spark.yarn.appMasterEnv.ALLUXIO_FUSE_HOME", appConf.get(SPARK_LIVY_ALLUXIO_HOME))
      appConf.put("spark.yarn.appMasterEnv.ALLUXIO_FUSE_JAVA_OPTS",
          appConf.getOrDefault(ALLUXIO_FUSE_JAVA_OPTS, ""))
      appConf.put("spark.yarn.appMasterEnv.ALLUXIO_JAVA_OPTS",
          appConf.getOrDefault(ALLUXIO_JAVA_OPTS, ""))
    }

    if (archives.nonEmpty) {
      appConf.put("spark.archives", archives.filter(_.nonEmpty).mkString(","))
    }
  }
}
