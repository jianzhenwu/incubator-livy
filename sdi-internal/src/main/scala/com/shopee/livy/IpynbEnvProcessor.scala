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

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.shopee.livy.IpynbEnvProcessor.{HADOOP_USER_NAME, HADOOP_USER_RPCPASSWORD, SPARK_LIVY_IPYNB_ARCHIVES, SPARK_LIVY_IPYNB_ENV_ENABLED, SPARK_LIVY_IPYNB_FILES, SPARK_LIVY_IPYNB_JARS, SPARK_LIVY_IPYNB_PY_FILES}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

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

  private[livy] var mockFileSystem: Option[FileSystem] = None
}

class IpynbEnvProcessor extends ApplicationEnvProcessor with Logging {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {

    val configuration = new Configuration()
    configuration.set("fs.s3a.impl.disable.cache", "false")

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
    val ipynbFiles = appConf.get(SPARK_LIVY_IPYNB_FILES)
    val ipynbArchives = appConf.get(SPARK_LIVY_IPYNB_ARCHIVES)
    val ipynbPyFiles = appConf.get(SPARK_LIVY_IPYNB_PY_FILES)

    val ipynbPackages = Array(
      (SPARK_JARS, ipynbJars),
      (SPARK_FILES, ipynbFiles),
      (SPARK_ARCHIVES, ipynbArchives),
      (SPARK_PY_FILES, ipynbPyFiles))

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

    appConf.asScala.filter(kv => kv._1.startsWith("spark.hadoop.fs"))
      .foreach {
        kv => configuration.set(kv._1.substring("spark.hadoop.".length), kv._2)
      }

    val nonEmptyPackages = ipynbPackages
      .filter(kv => StringUtils.isNotBlank(kv._2))

    if (nonEmptyPackages.nonEmpty) {
      val fs = IpynbEnvProcessor.mockFileSystem.getOrElse(
        new Path(nonEmptyPackages.head._2).getFileSystem(configuration)
      )
      nonEmptyPackages
        .foreach(kv => this.searchPackages(kv._2, appConf, kv._1, fs))

      fs.close()
    }
  }

  def searchPackages(packagesPaths: String, appConf: java.util.Map[String, String],
      key: String, fs: FileSystem): Unit = {

    val files = new ArrayBuffer[String]()
    // s3a://bucket_a/jars/*.jar => packagesDir=s3a://bucket_a/jars/, others=*.jar
    val regex = "(s3a://[^*]+)([*]*.*)".r

    packagesPaths.trim match {
      case regex(packagesDir, others) =>
        info(s"Ipynb packages directory $packagesDir, others=$others")
        val path = new Path(packagesDir)
        if (!fs.exists(path)) return
        fs.listStatus(path).foreach(fileStatus => {
          files += fileStatus.getPath.toString
        })
        files += appConf.getOrDefault(key, "")
        appConf.put(key, files.filter(_.nonEmpty).mkString(","))
      case _ =>
        warn(s"Fail to identify ipynb packages directory from $packagesPaths")
    }
  }
}
