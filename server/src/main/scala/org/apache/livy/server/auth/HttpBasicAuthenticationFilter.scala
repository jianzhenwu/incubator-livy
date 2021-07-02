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
import javax.servlet.{Filter, FilterChain, FilterConfig, ServletRequest, ServletResponse}
import javax.servlet.http.{HttpServletRequest, HttpServletRequestWrapper, HttpServletResponse}

import org.apache.commons.codec.binary.Base64

object HttpBasicAuthenticationHolder {
  private val holder = new ThreadLocal[Option[(String, String)]]() {
    override def initialValue(): Option[(String, String)] = {
      None
    }
  }

  def clear(): Unit = {
    holder.remove()
  }

  def set(usernamePassword: Option[(String, String)]): Unit = {
    holder.set(usernamePassword)
  }

  def get(): Option[(String, String)] = {
    holder.get()
  }
}

class HttpBasicAuthenticationFilter extends Filter {

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

    // TODO Temp solution, refactor to DMP auth later
    HttpBasicAuthenticationHolder.set(basicUsernamePassword(httpRequest))
    try {
      if (HttpBasicAuthenticationHolder.get().isDefined) {
        val authHttpRequest = new HttpServletRequestWrapper(httpRequest) {
          override def getAuthType: String = BASIC
          override def getRemoteUser: String = HttpBasicAuthenticationHolder.get().get._1
        }
        chain.doFilter(authHttpRequest, response)
      } else {
        chain.doFilter(request, response)
      }
    } finally {
      HttpBasicAuthenticationHolder.clear()
    }
  }

  private def basicUsernamePassword(httpRequest: HttpServletRequest) = {
    val authorization = httpRequest.getHeader(AUTHORIZATION_HEADER)
    if (authorization != null && !authorization.isEmpty) {
      val base64 = new Base64(0)
      val credentialsPart = authorization.substring(BASIC.length).trim
      val credentials = new String(base64.decode(credentialsPart), StandardCharsets.UTF_8)
        .split(":", 2)
      if (credentials.length == 2) {
        Some((credentials(0), credentials(1)))
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
