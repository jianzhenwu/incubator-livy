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

import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.classTag
import scala.util.{Failure, Success, Try}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.shopee.livy.auth.DmpAuthentication._
import okhttp3.{Headers, HttpUrl, MediaType, OkHttpClient, Request, RequestBody}
import org.apache.http.HttpStatus

import org.apache.livy.Logging

/**
 * DMP Hadoop account authentication for Livy.
 */
class DmpAuthentication(serverToken: String, serverHost: String) extends Logging {

  private var httpClient: OkHttpClient = new OkHttpClient()

  private val objectMapper: ObjectMapper = new ObjectMapper()
    .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  private val headers: Map[String, String] =
    Map("Content-type" -> "application/json", "X-DMP-Authorization" -> serverToken)

  // Visible for testing
  private[auth] def mock(mockClient: OkHttpClient): Unit = {
    httpClient = mockClient
  }

  def getPassword(hadoopAccount: String): String = {
    require(hadoopAccount != null, s"Hadoop account $hadoopAccount must exist")

    val url = new HttpUrl.Builder()
      .scheme("https")
      .host(serverHost)
      .encodedPath(s"$passwordPath$hadoopAccount")
      .build()
    val responseView: Try[HadoopAccountResponseView] =
      doAuthGet[HadoopAccountResponseView](url, headers.asJava)

    if (responseView.isSuccess) {
      responseView.get.data
    } else {
      error(s"Internal authentication server error, " +
        s"Error message: ${responseView.failed.get.getMessage}.")
      throw new AuthClientException(s"Internal authentication server error, " +
        s"Error message: ${responseView.failed.get.getMessage}.")
    }
  }

  def validate(hadoopAccount: String, password: String): Boolean = {
    val userInfo = HadoopAccount(hadoopAccount, password)
    val url = new HttpUrl.Builder()
      .scheme("https")
      .host(serverHost)
      .encodedPath(s"$validatePath")
      .build()
    val body = RequestBody.create(
      objectMapper.writeValueAsString(userInfo),
      MediaType.parse("application/json"))
    val responseView: Try[ValidateResponseView] =
      doAuthPost[ValidateResponseView](url, headers.asJava, body)

    if (responseView.isSuccess) {
      responseView.get.data
    } else {
      error(s"Internal authentication server error, " +
        s"Error message: ${responseView.failed.get.getMessage}.")
      throw new AuthClientException(s"Internal authentication server error, " +
        s"Error message: ${responseView.failed.get.getMessage}.")
    }
  }

  private def doAuthRaw(
      url: HttpUrl,
      method: String,
      headers: java.util.Map[String, String],
      body: Option[RequestBody]): String = {
    val requestBuilder = new Request.Builder()
    if (headers != null && !headers.isEmpty) {
      requestBuilder.headers(Headers.of(headers))
    }
    val req = requestBuilder.method(method, body.orNull).url(url).build()
    var retryCnt = 3
    while(retryCnt > 0) {
      retryCnt -= 1
      val res = httpClient.newCall(req).execute()
      val traceId = res.header("trace-id")
      val statusCode = res.code()
      val resBody = res.body().string()

      if (statusCode / 100 * 100 == HttpStatus.SC_OK) {
        res.body().close()
        return resBody
      } else {
        // log trace-id and retry.
        error(s"RAM authentication failed. statusCode=$statusCode, " +
          s"trace-id=$traceId, responseBody=$resBody")
      }
    }
    throw new AuthClientException("RAM authentication failed.")
  }

  private def doAuthGet[T: ClassTag](
      url: HttpUrl,
      headers: java.util.Map[String, String]): Try[T] = {
    try {
      val resBody = this.doAuthRaw(url, "GET", headers, None)
      Success(objectMapper.readValue(resBody, classTag[T].runtimeClass).asInstanceOf[T])
    } catch {
      case exception: Throwable => Failure(exception)
    }
  }

  private def doAuthPost[T: ClassTag](
      url: HttpUrl,
      headers: java.util.Map[String, String],
      body: RequestBody): Try[T] = {
    try {
      val resBody = this.doAuthRaw(url, "POST", headers, Option(body))
      Success(objectMapper.readValue(resBody, classTag[T].runtimeClass).asInstanceOf[T])
    } catch {
      case exception: Throwable => Failure(exception)
    }
  }
}

object DmpAuthentication {

  private val SERVER_TOKEN = "LIVY_SERVER_AUTH_SERVER_TOKEN"

  private val SERVER_HOST = "LIVY_SERVER_AUTH_SERVER_HOST"

  private val passwordPath = "/ram/api/v1/developer/sensitive/hadoopAccount/pwd/"

  private val validatePath = "/ram/api/v1/developer/sensitive/hadoopAccount/validate"

  private val DMP_AUTHENTICATION_CONSTRUCTOR_LOCK = new Object()

  private val dmpAuthentication: AtomicReference[DmpAuthentication] =
    new AtomicReference[DmpAuthentication](null)

  def apply(): DmpAuthentication = {
    DMP_AUTHENTICATION_CONSTRUCTOR_LOCK.synchronized {
      if (dmpAuthentication.get() == null) {
        dmpAuthentication.set(
          new DmpAuthentication(System.getenv(SERVER_TOKEN), System.getenv(SERVER_HOST)))
      }
      dmpAuthentication.get()
    }
  }
}

case class HadoopAccount(account: String, password: String)

case class HadoopAccountResponseView(code: Int, message: String, data: String)

case class ValidateResponseView(code: Int, message: String, data: Boolean)

class AuthClientException(message: String) extends RuntimeException(message)
