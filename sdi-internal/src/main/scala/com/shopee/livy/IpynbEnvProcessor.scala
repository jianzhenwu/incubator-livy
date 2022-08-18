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

import scala.collection.mutable

import com.shopee.livy.IpynbEnvProcessor.{HADOOP_USER_NAME, HADOOP_USER_RPCPASSWORD, SPARK_LIVY_IPYNB_ARCHIVES, SPARK_LIVY_IPYNB_ENV_ENABLED, SPARK_LIVY_IPYNB_FILES, SPARK_LIVY_IPYNB_JARS, SPARK_LIVY_IPYNB_PY_FILES}
import org.apache.commons.lang3.StringUtils

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, Logging}
import org.apache.livy.LivyConf.{SPARK_ARCHIVES, SPARK_FILES, SPARK_JARS, SPARK_PY_FILES}

object IpynbEnvProcessor {
  val SPARK_LIVY_IPYNB_JARS = "spark.livy.ipynb.jars"
  val SPARK_LIVY_IPYNB_FILES = "spark.livy.ipynb.files"
  val SPARK_LIVY_IPYNB_ARCHIVES = "spark.livy.ipynb.archives"
  val SPARK_LIVY_IPYNB_PY_FILES = "spark.livy.ipynb.pyFiles"

  val SPARK_LIVY_IPYNB_ENV_ENABLED = "spark.livy.ipynb.env.enabled"

  val HADOOP_USER_NAME = "HADOOP_USER_NAME"
  val HADOOP_USER_RPCPASSWORD = "HADOOP_USER_RPCPASSWORD"
}

class IpynbEnvProcessor extends ApplicationEnvProcessor with Logging {
  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {

    val appConf = applicationEnvContext.appConf
    val env = applicationEnvContext.env

    val envEnabled = appConf.getOrDefault(SPARK_LIVY_IPYNB_ENV_ENABLED, "false")
    if ("false".equalsIgnoreCase(envEnabled)) {
      return
    }

    val sparkHome = env.get("SPARK_HOME").trim.stripSuffix("/")
    val pysparkZip = s"$sparkHome/python/lib/pyspark.zip"
    val py4jZip = s"$sparkHome/python/lib/py4j-*.zip"

    appConf.put(SPARK_PY_FILES,
      s"$pysparkZip,$py4jZip" + appConf.getOrDefault(SPARK_PY_FILES, ""))

    val ipynbJars = appConf.get(SPARK_LIVY_IPYNB_JARS)
    Option(ipynbJars).filter(_.nonEmpty).foreach(jars =>
      appConf.put(SPARK_JARS,
        appConf.getOrDefault(SPARK_JARS, "") + "," + jars.trim))
    val ipynbFiles = appConf.get(SPARK_LIVY_IPYNB_FILES)
    Option(ipynbFiles).filter(_.nonEmpty).foreach(files =>
      appConf.put(SPARK_FILES,
        appConf.getOrDefault(SPARK_FILES, "") + "," + files.trim))
    val ipynbArchives = appConf.get(SPARK_LIVY_IPYNB_ARCHIVES)
    Option(ipynbArchives).filter(_.nonEmpty).foreach(archives =>
      appConf.put(SPARK_ARCHIVES,
        appConf.getOrDefault(SPARK_ARCHIVES, "") + "," + archives.trim))
    val ipynbPyFiles = appConf.get(SPARK_LIVY_IPYNB_PY_FILES)
    Option(ipynbPyFiles).filter(_.nonEmpty).foreach(pyFiles =>
      appConf.put(SPARK_PY_FILES,
        appConf.getOrDefault(SPARK_PY_FILES, "") + "," + pyFiles.trim))

    val bucketNames = new mutable.HashSet[String]()
    Seq(ipynbJars, ipynbFiles, ipynbArchives, ipynbPyFiles)
      .filter(StringUtils.isNotBlank(_))
      .filter(_.startsWith("s3a://"))
      .filter(_.length > 6)
      .foreach(f =>
        bucketNames += f.substring(6)
          .split("/")
          .head
      )
    bucketNames.foreach { bucketName =>
      appConf.putIfAbsent(s"spark.hadoop.fs.s3a.bucket.$bucketName.access.key",
        env.get(HADOOP_USER_NAME))
      appConf.putIfAbsent(s"spark.hadoop.fs.s3a.bucket.$bucketName.secret.key",
        env.get(HADOOP_USER_RPCPASSWORD))
    }
  }
}
