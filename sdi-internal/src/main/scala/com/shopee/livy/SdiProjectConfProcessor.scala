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

import com.shopee.livy.auth.DmpAuthentication

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, Logging}
import org.apache.livy.client.common.ClientConf

object SdiProjectConfProcessor {
  val SPARK_SHOPEE_DATA_INFRA_PROJECT = "spark.shopee.datainfra.project"
  val SPARK_SHOPEE_DATA_INFRA_PROJECT_ACCOUNT = "spark.shopee.datainfra.project.account"
  val SPARK_SHOPEE_DATA_INFRA_PROJECT_PASSWORD = "spark.shopee.datainfra.project.password"

  // For testing
  private[livy] var mockDmpAuthentication: DmpAuthentication = _

  lazy val dmpAuthentication: DmpAuthentication =
    if (mockDmpAuthentication != null) {
      mockDmpAuthentication
    } else {
      DmpAuthentication()
    }
}

class SdiProjectConfProcessor extends ApplicationEnvProcessor with Logging {

  import SdiProjectConfProcessor._

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    val appConf = applicationEnvContext.appConf

    val username = appConf.get(ClientConf.LIVY_APPLICATION_HADOOP_USER_NAME_KEY)
    val projectCode = appConf.get(SPARK_SHOPEE_DATA_INFRA_PROJECT)
    if (projectCode != null &&
      (username.equals(projectCode) || dmpAuthentication.belongProject(username, projectCode))) {
      val additionalPropertiesMap = dmpAuthentication.getAdditionalProperties(Seq(projectCode))

      additionalPropertiesMap.values.foreach(properties => {
        appConf.put(SPARK_SHOPEE_DATA_INFRA_PROJECT_ACCOUNT, properties.prodServiceAccount)
        appConf.put(SPARK_SHOPEE_DATA_INFRA_PROJECT_PASSWORD, properties.prodServicePassword)
      })
    }
  }
}
