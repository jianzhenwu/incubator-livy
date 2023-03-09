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

import org.apache.commons.lang3.StringUtils

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, Logging}
import org.apache.livy.utils.LivyProcessorException

object DockerEnvProcessor {

  val SPARK_LIVY_DOCKER_ENABLED: String = "spark.livy.docker.enabled"
  val SPARK_LIVY_DOCKER_IMAGE: String = "spark.livy.docker.image"
  val SPARK_DOCKER_MOUNTS: String = "livy.rsc.spark.docker.mounts"

  def isDockerEnabled(appConf: java.util.Map[String, String]): Boolean = {
    Option(appConf.get(SPARK_LIVY_DOCKER_ENABLED)).exists("true".equalsIgnoreCase)
  }
}

class DockerEnvProcessor extends ApplicationEnvProcessor with Logging {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {

    import DockerEnvProcessor._

    val appConf = applicationEnvContext.appConf
    val dockerImage = appConf.get(SPARK_LIVY_DOCKER_IMAGE)
    val dockerMounts = appConf.get(SPARK_DOCKER_MOUNTS)

    if (isDockerEnabled(appConf)) {
      if (StringUtils.isBlank(dockerImage)) {
        error(s"Please check conf $SPARK_LIVY_DOCKER_IMAGE, " +
          s"Yarn container runtime docker image must be set by user")
        throw new LivyProcessorException(s"$SPARK_LIVY_DOCKER_IMAGE must be set by user")
      }
      appConf.put("spark.executorEnv.YARN_CONTAINER_RUNTIME_TYPE", "docker")
      appConf.put("spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE", dockerImage)
      appConf.put("spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS", dockerMounts)
      appConf.put("spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_TYPE", "docker")
      appConf.put("spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE", dockerImage)
      appConf.put("spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS", dockerMounts)
      appConf.put("spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_DEVICES", "/dev/fuse")
    }
  }
}
