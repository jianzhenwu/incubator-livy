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

package com.shopee.livy.auth

import java.nio.charset.StandardCharsets
import javax.security.sasl.AuthenticationException
import javax.servlet.{Filter, FilterChain, FilterConfig, ServletRequest, ServletResponse}
import javax.servlet.http.{HttpServletRequest, HttpServletRequestWrapper, HttpServletResponse}

import org.apache.commons.codec.binary.Base64

class SdiHttpBasicAuthenticationFilter extends Filter {

  val BASIC = "Basic"
  val AUTHORIZATION_HEADER = "Authorization"

  override def init(filterConfig: FilterConfig): Unit = {

  }

  override def doFilter(
      request: ServletRequest,
      response: ServletResponse,
      chain: FilterChain): Unit = {
    val httpRequest = request.asInstanceOf[HttpServletRequest]
    val httpResponse = response.asInstanceOf[HttpServletResponse]

    val userInfo = basicUsernamePassword(httpRequest)

    val requestUser = userInfo.fold {
      "anonymous"
    } { case (username, password) =>
      val isAuth = DmpAuthentication().validate(username, password)
      if (!isAuth) {
        throw new AuthenticationException(s"Unauthorized user $username.")
      }
      username
    }
    val authHttpRequest = new HttpServletRequestWrapper(httpRequest) {
      override def getAuthType: String = BASIC

      override def getRemoteUser: String = requestUser
    }
    chain.doFilter(authHttpRequest, response)
  }

  private def basicUsernamePassword(httpRequest: HttpServletRequest) = {
    val authorization = httpRequest.getHeader(AUTHORIZATION_HEADER)
    if (authorization != null && !authorization.isEmpty) {
      val base64 = new Base64(0)
      val credentialsPart = authorization.substring(BASIC.length).trim
      val credentials = new String(base64.decode(credentialsPart), StandardCharsets.UTF_8)
        .split(":", 2)
      if (credentials.length == 2) {
        Some((credentials(0).trim, credentials(1).trim))
      } else if (credentials.length == 1) {
        throw new AuthenticationException(s"Unauthorized user ${credentials(0).trim}.")
      } else {
        None
      }
    } else {
      None
    }
  }

  override def destroy(): Unit = {

  }
}
