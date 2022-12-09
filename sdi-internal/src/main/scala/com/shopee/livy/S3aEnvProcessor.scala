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

import java.io.File
import java.nio.file.Paths
import java.util.Locale

import com.shopee.livy.S3aEnvProcessor.{EXCLUSIONS, REPOSITORIES, S3A_CHANGE_DETECTION_MODE, S3A_CHANGE_DETECTION_VERSION_REQUIRED, S3A_PATH_STYLE_ACCESS}
import com.shopee.livy.utils.IvyUtils.{buildIvySettings, resolveMavenCoordinates}
import org.apache.commons.io.filefilter.PrefixFileFilter

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, Logging}
import org.apache.livy.client.common.ClientConf
import org.apache.livy.utils.LivyProcessorException

object S3aEnvProcessor {

  val SPARK_LIVY_S3A_ENABLED: String = "spark.livy.s3a.enabled"

  private val REPOSITORIES: String =
    "https://di-nexus-repo.idata.shopeemobile.com/repository/maven-release/," +
      "https://maven-central.storage-download.googleapis.com/maven2/"

  private val EXCLUSIONS = Seq("org.wildfly.openssl:wildfly-openssl")

  private val S3A_CHANGE_DETECTION_VERSION_REQUIRED =
    "spark.hadoop.fs.s3a.change.detection.version.required"
  private val S3A_CHANGE_DETECTION_MODE =
    "spark.hadoop.fs.s3a.change.detection.mode"
  val S3A_PATH_STYLE_ACCESS =
    "spark.hadoop.fs.s3a.path.style.access"
}

class S3aEnvProcessor extends ApplicationEnvProcessor with Logging {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {

    val env = applicationEnvContext.env
    val appConf = applicationEnvContext.appConf

    val s3aEnabled = appConf.get(S3aEnvProcessor.SPARK_LIVY_S3A_ENABLED)

    Option(s3aEnabled).filter("true".equalsIgnoreCase).foreach { _ =>
      appConf.put(S3A_PATH_STYLE_ACCESS, "true")

      val sparkHome = env.get("SPARK_HOME")

      val sparkHomeFile = new File(sparkHome)
      val sparkHomeName = if (sparkHomeFile.exists()) {
        Some(sparkHomeFile.toPath.toRealPath().toFile.getName)
      } else {
        // For unit test.
        Some(sparkHomeFile.getName)
      }

      sparkHomeName.foreach { dirName =>
        // Version example: spark-3.2.1-sdi-006-bin-3.3.sdi-011
        val regex = "^spark-([\\d]+[.][\\d]+)[.][\\d]+-sdi-[\\d]+-bin-(\\S+)$".r
        dirName.toLowerCase(Locale.ENGLISH) match {
          case regex(sparkVersion, hadoopVersion) =>
            // We embeded Hadoop dependencies into Spark distribution
            val awsArtifacts = s"org.apache.hadoop:hadoop-aws:$hadoopVersion"

            // should set spark.jars.ivy in livy spark-defaults.conf
            val ivyPath: Option[String] = Option(appConf.get("spark.jars.ivy")).fold {
              throw new LivyProcessorException("The value of spark.jars.ivy cannot " +
                "be empty when the s3a feature is enabled. " +
                "Please make sure the parent folder doesn't start with a dot.")
            } { e => Option(Paths.get(e, hadoopVersion).toString) }

            val ivyJars = Paths.get(ivyPath.get, "jars").toFile
            val hadoopAws = ivyJars.list(
              new PrefixFileFilter("org.apache.hadoop_hadoop-aws-"))
            val amazonAws = ivyJars.list(
              new PrefixFileFilter("com.amazonaws_aws-java-sdk-bundle-"))

            val jars = if (hadoopAws != null && amazonAws != null
              && hadoopAws.nonEmpty && amazonAws.nonEmpty) {
              s"${ivyJars.toPath.resolve(hadoopAws(0))}," +
                s"${ivyJars.toPath.resolve(amazonAws(0))}"
            } else {
              // It takes a few seconds.
              resolveMavenCoordinates(
                awsArtifacts,
                buildIvySettings(Some(REPOSITORIES), ivyPath),
                EXCLUSIONS)
            }

            appConf.put("spark.jars",
              s"$jars,${appConf.getOrDefault("spark.jars", "")}")

            // Ozone does not support detection
            appConf.put(S3A_CHANGE_DETECTION_VERSION_REQUIRED,
              s"${appConf.getOrDefault(S3A_CHANGE_DETECTION_VERSION_REQUIRED, "false")}")
            appConf.put(S3A_CHANGE_DETECTION_MODE,
              s"${appConf.getOrDefault(S3A_CHANGE_DETECTION_MODE, "warn")}")

            // Add aws package in classpath in order to download resources
            // from Ozone.
            env.put("SPARK_DIST_CLASSPATH", "$SPARK_DIST_CLASSPATH:" +
              s"${jars.replace(",", ":")}")
            info(s"Set SPARK_DIST_CLASSPATH = ${env.get("SPARK_DIST_CLASSPATH")}")
          case _ =>
            throw new LivyProcessorException(
              s"Hadoop version not recognized. sparkHome = $sparkHome")
        }
      }
    }
  }

}
