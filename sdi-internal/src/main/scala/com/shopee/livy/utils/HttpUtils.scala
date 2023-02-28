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
package com.shopee.livy.utils

import scala.collection.JavaConverters._
import scala.reflect.{classTag, ClassTag}
import scala.util.{Failure, Success, Try}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import okhttp3.{Headers, HttpUrl, OkHttpClient, Request, RequestBody}
import org.apache.http.HttpStatus

import org.apache.livy.Logging

class RequestException(message: String) extends RuntimeException(message)

object HttpUtils extends Logging {

  // For testing
  private[livy] var mockHttpClient: OkHttpClient = _

  lazy val httpClient: OkHttpClient = {
    if (mockHttpClient != null) {
      mockHttpClient
    } else {
      new OkHttpClient()
    }
  }

  private val objectMapper: ObjectMapper = new ObjectMapper()
    .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def doGet[T: ClassTag](
      url: HttpUrl,
      headers: Map[String, String],
      exceptStatusCodes: Option[Set[Int]] = None): Try[T] = {
    try {
      val resBody = doRequest(url, "GET", headers, None, exceptStatusCodes)
      Success(objectMapper.readValue(resBody, classTag[T].runtimeClass).asInstanceOf[T])
    } catch {
      case exception: Throwable => Failure(exception)
    }
  }

  def doPost[T: ClassTag](
      url: HttpUrl,
      headers: Map[String, String],
      body: RequestBody,
      exceptStatusCodes: Option[Set[Int]] = None): Try[T] = {
    try {
      val resBody = doRequest(url, "POST", headers, Option(body), exceptStatusCodes)
      Success(objectMapper.readValue(resBody, classTag[T].runtimeClass).asInstanceOf[T])
    } catch {
      case exception: Throwable => Failure(exception)
    }
  }

  private def doRequest(
      url: HttpUrl,
      method: String,
      headers: Map[String, String],
      body: Option[RequestBody],
      exceptStatusCodes: Option[Set[Int]] = None): String = {
    val requestBuilder = new Request.Builder()
    if (headers != null && headers.nonEmpty) {
      requestBuilder.headers(Headers.of(headers.asJava))
    }
    val req = requestBuilder.method(method, body.orNull).url(url).build()
    var retryCnt = 3
    while(retryCnt > 0) {
      retryCnt -= 1
      val res = httpClient.newCall(req).execute()
      val traceId = res.header("trace-id")
      val statusCode = res.code()
      val resBody = res.body().string()
      res.body().close()
      if (statusCode / 100 * 100 == HttpStatus.SC_OK ||
          exceptStatusCodes.getOrElse(Set[Int]()).contains(statusCode)) {
        return resBody
      } else {
        // log trace-id and retry.
        error(s"Error request with url $url. statusCode=$statusCode, " +
          s"trace-id=$traceId, responseBody=$resBody")
      }
    }
    throw new RequestException(s"Error request with url: $url")
  }
}
