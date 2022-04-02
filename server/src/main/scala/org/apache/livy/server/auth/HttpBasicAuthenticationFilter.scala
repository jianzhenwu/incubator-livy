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

import java.nio.charset.StandardCharsets
import javax.security.sasl.AuthenticationException
import javax.servlet.{Filter, FilterChain, FilterConfig, ServletRequest, ServletResponse}
import javax.servlet.http.{HttpServletRequest, HttpServletRequestWrapper, HttpServletResponse}

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.codec.binary.Base64

import org.apache.livy.metrics.common.{Metrics, MetricsKey}

class HttpBasicAuthenticationFilter extends Filter {

  val BASIC = "Basic"
  val AUTHORIZATION_HEADER = "Authorization"

  private val objectMapper: ObjectMapper = new ObjectMapper()
    .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)

  private var provider: AuthenticationProvider = _

  override def init(filterConfig: FilterConfig): Unit = {
    val providerName = filterConfig.getInitParameter("authentication.provider")
    provider = AuthenticationProvider.apply(providerName)
  }

  override def doFilter(
      request: ServletRequest,
      response: ServletResponse,
      chain: FilterChain): Unit = {
    val httpRequest = request.asInstanceOf[HttpServletRequest]
    val httpResponse = response.asInstanceOf[HttpServletResponse]

    var requestUser = "anonymous"
    try {
      val userInfo = basicUsernamePassword(httpRequest)
      userInfo.foreach {
        case (username, password) =>
          Metrics().incrementCounter(MetricsKey.CUSTOMIZE_AUTHENTICATION_TOTAL_COUNT)
          provider.authenticate(username, password)
          requestUser = username
      }
    } catch {
      case ae: AuthenticationException =>
        Metrics().incrementCounter(MetricsKey.CUSTOMIZE_AUTHENTICATION_FAILED_COUNT)
        writeResponse(
          httpResponse,
          HttpServletResponse.SC_UNAUTHORIZED,
          objectMapper.writeValueAsString(ae.getMessage))
        return
      case e =>
        Metrics().incrementCounter(MetricsKey.CUSTOMIZE_AUTHENTICATION_ERROR_COUNT)
        writeResponse(
          httpResponse,
          HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          objectMapper.writeValueAsString(e.getMessage))
        return
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

  private def writeResponse(response: HttpServletResponse, code: Int, message: String): Unit = {
    response.setStatus(code)
    response.getWriter.write(message)
    response.getWriter.flush()
  }

  override def destroy(): Unit = {

  }
}
