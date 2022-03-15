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

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, Logging}
import org.apache.livy.client.common.ClientConf

object S3aEnvProcessor {
  val SPARK_S3A_ENABLED: String = "spark.s3a.enabled"
}

class S3aEnvProcessor extends ApplicationEnvProcessor with Logging {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {

    val env = applicationEnvContext.env
    val sparkConfDir = applicationEnvContext.appConf
      .get(ClientConf.LIVY_APPLICATION_SPARK_CONF_DIR_KEY)
    val s3aEnabled = applicationEnvContext.appConf
      .get(S3aEnvProcessor.SPARK_S3A_ENABLED)

    Option(s3aEnabled).filter("true".equalsIgnoreCase).foreach { _ => {
      var classPath = "$HADOOP_CLASSPATH"
      Option(sparkConfDir).orElse(Option(env.get("SPARK_CONF_DIR")))
        .filter(_.nonEmpty).foreach {
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
    }}
  }

}
