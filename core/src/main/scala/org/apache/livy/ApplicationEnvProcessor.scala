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

package org.apache.livy

import java.io.File

import scala.collection.JavaConverters.{mapAsScalaMapConverter, propertiesAsScalaMapConverter}

import org.apache.livy.client.common.ClientConf

case class ApplicationEnvContext(env: java.util.Map[String, String],
    appConf: java.util.Map[String, String], sessionType: Option[SessionType] = None)

trait ApplicationEnvProcessor {

  def process(applicationEnvContext: ApplicationEnvContext)

}

class DefaultHadoopEnvProcessor extends ApplicationEnvProcessor with Logging {
  override def process(
      applicationEnvContext: ApplicationEnvContext): Unit = {
  }
}

class DefaultSparkEnvProcessor extends ApplicationEnvProcessor with Logging {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    val appConf = applicationEnvContext.appConf
    val sparkDefaults =
      ClassLoaderUtils.loadAsPropertiesFromClasspath("spark-defaults.conf")
    if (null != sparkDefaults) {
      sparkDefaults.asScala.foreach(kv => {
        if (!appConf.containsKey(kv._1)) {
          appConf.put(kv._1, kv._2)
        }
      })
    }

    val sparkConfDir = appConf.asScala.get(ClientConf.LIVY_APPLICATION_SPARK_CONF_DIR_KEY)

    sparkConfDir.foreach(dir => {
      val sparkDefaults = new File(dir + File.separator + "spark-defaults.conf")
      if (sparkDefaults.isFile) {
        val props = Utils.getPropertiesFromFile(sparkDefaults)
        props.foreach(kv => {
          if (!appConf.containsKey(kv._1)) {
            appConf.put(kv._1, kv._2)
          }
        })
      } else {
        logger.error(s"Fail to load ${sparkDefaults.getAbsolutePath}")
      }
    })
  }

}

object ApplicationEnvProcessor {

  val SPARK_AUX_JAR = "spark.aux.jar"

  def apply(clazz: String): ApplicationEnvProcessor = {
    Class.forName(clazz)
      .asSubclass(classOf[ApplicationEnvProcessor])
      .getConstructor()
      .newInstance()
  }
}
