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
import scala.collection.mutable.ArrayBuffer

import com.google.common.net.UrlEscapers
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

  val OZONE_SERVICE_IDS = "spark.livy.ozone.service.ids"
  val WORKSPACE_BUCKET_IDC_REGION = "spark.livy.ipynb.workspace.bucket.idc-region"
  val DEFAULT_WORKSPACE_BUCKET_IDC_REGION = "sg"

  val SPARK_LIVY_IPYNB_ENV_ENABLED = "spark.livy.ipynb.env.enabled"

  val SPARK_PYSPARK_PYTHON = "spark.pyspark.python"

  val HADOOP_USER_NAME = "HADOOP_USER_NAME"
  val HADOOP_USER_RPCPASSWORD = "HADOOP_USER_RPCPASSWORD"

  private[livy] var mockFileSystem: Option[FileSystem] = None
}

class IpynbEnvProcessor extends ApplicationEnvProcessor with Logging {

  import IpynbEnvProcessor._

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {

    val appConf = applicationEnvContext.appConf
    val env = applicationEnvContext.env

    val envEnabled = appConf.getOrDefault(SPARK_LIVY_IPYNB_ENV_ENABLED, "false")
    if ("false".equalsIgnoreCase(envEnabled)) {
      return
    }

    val configuration = new Configuration()
    configuration.set("fs.s3a.impl.disable.cache", "true")

    val sparkHome = env.get("SPARK_HOME").trim.stripSuffix("/")

    appConf.put("spark.sql.auth.canFailJob", "true")
    val pyfiles = new ArrayBuffer[String]()
    pyfiles += s"$sparkHome/python/lib/pyspark.zip"
    pyfiles += s"$sparkHome/python/lib/py4j-*.zip"
    pyfiles += appConf.get(SPARK_PY_FILES)

    appConf.put(SPARK_PY_FILES, pyfiles.filter(StringUtils.isNotBlank(_)).mkString(","))

    val ipynbJars = appConf.get(SPARK_LIVY_IPYNB_JARS)
    val ipynbFiles = appConf.get(SPARK_LIVY_IPYNB_FILES)
    val ipynbArchives = appConf.get(SPARK_LIVY_IPYNB_ARCHIVES)
    val ipynbPyFiles = appConf.get(SPARK_LIVY_IPYNB_PY_FILES)

    val ipynbPackages = Array(
      (SPARK_JARS, ipynbJars),
      (SPARK_FILES, ipynbFiles),
      (SPARK_ARCHIVES, ipynbArchives),
      (SPARK_PY_FILES, ipynbPyFiles))

    var bucketInfo: BucketInfo = BucketInfo("", "")
    Seq(ipynbJars, ipynbFiles, ipynbArchives, ipynbPyFiles).foreach(x => {
      if (bucketInfo.name.isEmpty) {
        val name = getBucketNameFromS3aPath(x)
        if (name.isDefined && name.nonEmpty) {
          bucketInfo = BucketInfo(x, name.get)
        }
      }
    })

    if (!bucketInfo.name.isEmpty) {
      val workspaceUri = getWorkspaceUriFromBucketInfo(bucketInfo,
          appConf.getOrDefault(WORKSPACE_BUCKET_IDC_REGION, DEFAULT_WORKSPACE_BUCKET_IDC_REGION),
          appConf.get(OZONE_SERVICE_IDS))
      appConf.put("spark.yarn.appMasterEnv.NOTEBOOK_ALLUXIO_WORKSPACE_URI", workspaceUri)
      appConf.putIfAbsent(s"spark.hadoop.fs.s3a.bucket.${bucketInfo.name}.access.key",
        env.get(HADOOP_USER_NAME))
      appConf.putIfAbsent(s"spark.hadoop.fs.s3a.bucket.${bucketInfo.name}.secret.key",
        env.get(HADOOP_USER_RPCPASSWORD))
    }

    appConf.asScala.filter(kv => kv._1.startsWith("spark.hadoop.fs"))
      .foreach {
        kv => configuration.set(kv._1.substring("spark.hadoop.".length), kv._2)
      }

    // Set default python binary executable to use for PySpark if user not set.
    if (DockerEnvProcessor.isDockerEnabled(appConf)) {
      appConf.putIfAbsent(SPARK_PYSPARK_PYTHON, "/usr/local/bin/python3")
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
        if (!fs.exists(path)) {
          warn(s"File is not exists. ${path.toString}")
          return
        }
        fs.listStatus(path).foreach(fileStatus => {
          val fileUrl =
            UrlEscapers.urlFragmentEscaper().escape(fileStatus.getPath.toString)
          files += fileUrl
          info(s"Add $fileUrl to $key")
        })
      case _ =>
        warn(s"Fail to identify ipynb packages directory from $packagesPaths, " +
          s"append it to $key")
        files += packagesPaths
    }
    files += appConf.getOrDefault(key, "")
    appConf.put(key, files.filter(_.nonEmpty).mkString(","))
  }

  def getBucketNameFromS3aPath(path: String): Option[String] = {
    if (StringUtils.isBlank(path) || !path.startsWith("s3a://") || path.length <= 6) {
      None
    } else {
      Some(path.substring(6).split("/").head)
    }
  }

  def getWorkspaceUriFromBucketInfo(bucketInfo: BucketInfo, bucketRegion: String,
      ozoneServiceIds: String): String = {
    val workspace = bucketInfo.path.split("/.notebook").head.split("/").last
    // get $project from $idcRegion-$project-notebook.
    val project = bucketInfo.name.stripPrefix(s"$bucketRegion-").stripSuffix("-notebook")
    val workspaceUri = new StringBuilder("/s3")
        .append("/")
        .append(ozoneServiceIds)
        .append("/")
        .append(project)
        .append("/")
        .append(bucketInfo.name)
        .append("/")
        .append("workspaces")
        .append("/")
        .append(workspace)
    workspaceUri.toString()
  }
}

case class BucketInfo(path: String, name: String)
