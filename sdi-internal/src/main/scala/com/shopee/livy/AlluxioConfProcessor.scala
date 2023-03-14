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

import com.shopee.livy.AlluxioConfProcessor.{ALLUXIO_FUSE_JAVA_OPTS, ALLUXIO_JAVA_OPTS, ALLUXIO_MEMORY_ALLOCATE, SPARK_LIVY_ALLUXIO_ARCHIVE, SPARK_LIVY_ALLUXIO_ENV_ENABLED}

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, ByteUnit, ByteUtils, Logging}

object AlluxioConfProcessor {
  val SPARK_LIVY_ALLUXIO_ENV_ENABLED = "spark.livy.alluxio.enabled"
  val SPARK_LIVY_ALLUXIO_ARCHIVE = "livy.rsc.spark.alluxio.archive"
  val ALLUXIO_FUSE_JAVA_OPTS = "livy.rsc.spark.alluxio.fuse.java.opts"
  val ALLUXIO_JAVA_OPTS = "livy.rsc.spark.alluxio.java.opts"
  val ALLUXIO_MEMORY_ALLOCATE = "livy.rsc.spark.alluxio.memory"
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
      appConf.put("spark.yarn.appMasterEnv.ALLUXIO_FUSE_JAVA_OPTS",
        appConf.getOrDefault(ALLUXIO_FUSE_JAVA_OPTS, ""))
      appConf.put("spark.yarn.appMasterEnv.ALLUXIO_JAVA_OPTS",
        appConf.getOrDefault(ALLUXIO_JAVA_OPTS, ""))
      allocateAlluxioMemInDriver(appConf)
    }

    if (archives.nonEmpty) {
      appConf.put("spark.archives", archives.filter(_.nonEmpty).mkString(","))
    }
  }

  def allocateAlluxioMemInDriver(appConf: java.util.Map[String, String]): String = {
    val curMemOverhead = appConf.get("spark.driver.memoryOverhead")
    var curMemOverHeadInMiB = {
      if (null == curMemOverhead || curMemOverhead.isEmpty) {
        0
      } else {
        ByteUtils.byteStringAs(curMemOverhead, ByteUnit.MiB)
      }
    }

    val alluxioMemInMiB =
      ByteUtils.byteStringAs(appConf.getOrDefault(ALLUXIO_MEMORY_ALLOCATE, "1024m"), ByteUnit.MiB)

    val newMemOverHeadInMiB = curMemOverHeadInMiB + alluxioMemInMiB
    appConf.put("spark.driver.memoryOverhead", newMemOverHeadInMiB + "M")
  }
}
