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

class SdiHadoopEnvProcessor extends ApplicationEnvProcessor with Logging{

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    val env = applicationEnvContext.env
    val username = applicationEnvContext.appConf.
      get(ClientConf.LIVY_APPLICATION_HADOOP_USER_NAME_KEY)

    if (username != null) {
      var password = ""
      try {
        password = DmpAuthentication.getPassword(username)
      } catch {
        case _: Exception =>
          error(s"Failed to get hadoop account password from DmpAuthentication " +
            s"with " + username)
      }
      env.put("HADOOP_USER_NAME", username)
      env.put("HADOOP_USER_RPCPASSWORD", password)
    }
  }
}
