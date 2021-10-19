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

package org.apache.livy.server.auth

import com.shopee.di.datasuite.auth.client.ClientCredentials
import com.shopee.di.datasuite.auth.client.common.EnvironmentEnum

/**
 * Set environment variable in livy-env.sh.
 */
class ShopeeClientCredentials extends ClientCredentials {
  override def getClientId: String = System.getenv("LIVY_SERVER_AUTH_DI_CLIENT_ID")

  override def getClientSecret: String = System.getenv("LIVY_SERVER_AUTH_CLIENT_SECRET")

  override def getEnvironment: EnvironmentEnum =
    EnvironmentEnum.valueOf(System.getenv("LIVY_SERVER_AUTH_ENVIRONMENT")
      .toUpperCase)
}
