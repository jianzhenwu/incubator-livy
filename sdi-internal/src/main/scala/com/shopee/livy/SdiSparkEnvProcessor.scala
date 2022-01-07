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

import java.util

import org.apache.livy.ApplicationEnvContext
import org.apache.livy.client.common.ClientConf

object SdiSparkEnvProcessor {
  val SPARK_S3A_ENABLED: String = "spark.s3a.enabled"
}

class SdiSparkEnvProcessor extends SdiHadoopEnvProcessor {

  override def process(
      applicationEnvContext: ApplicationEnvContext): Unit = {

    super.process(applicationEnvContext)

    this.processHadoopClasspathS3a(applicationEnvContext.env,
      applicationEnvContext.appConf.get(ClientConf.LIVY_APPLICATION_SPARK_CONF_DIR_KEY),
      applicationEnvContext.appConf.get(SdiSparkEnvProcessor.SPARK_S3A_ENABLED))
  }

  def processHadoopClasspathS3a(env: util.Map[String, String],
      sparkConfDir: String = null, s3aEnabled: String = null): Unit = {

    Option(s3aEnabled).filter("true".equalsIgnoreCase).foreach {
      _ => {
        var classPath = "$HADOOP_CLASSPATH"
        Option(sparkConfDir).orElse(Option(env.get("SPARK_CONF_DIR"))).filter(_.nonEmpty).foreach {
          conf =>
            import scala.sys.process._
            try {
              val hadoopHome = Seq("bash", "-c",
                "source " + conf + "/spark-env.sh; echo $HADOOP_HOME").!!
              if (hadoopHome != null && hadoopHome.nonEmpty) {
                classPath = "$HADOOP_CLASSPATH:" +
                  s"${hadoopHome.trim}/share/hadoop/tools/lib/hadoop-aws-*.jar:" +
                  s"${hadoopHome.trim}/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar"
              }
            } catch {
              case e: Exception =>
                error("Unable to source spark-env.sh", e)
            }
        }
        env.put("HADOOP_CLASSPATH", classPath)
        info(s"Set HADOOP_CLASSPATH = $classPath")
      }
    }
  }
}
