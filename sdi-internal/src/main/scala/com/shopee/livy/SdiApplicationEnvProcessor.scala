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

import com.shopee.di.datasuite.auth.client.BigDataAuthProxy

import org.apache.livy.{ApplicationEnvProcessor, Logging}

class SdiApplicationEnvProcessor extends ApplicationEnvProcessor with Logging {

  override def process(env: util.Map[String, String],
                       username: String): Unit = {

    var password = ""
    try {
      password = BigDataAuthProxy.getInstance.getHadoopAccountPassword(username)
    } catch {
      case _: Exception =>
        error(s"Failed to get hadoop account password from BigDataAuthProxy " +
          s"with " + username)
    }
    env.put("HADOOP_USER_NAME", username)
    env.put("HADOOP_USER_RPCPASSWORD", password)
  }
}
