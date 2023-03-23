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

import com.shopee.livy.utils.HttpUtils
import okhttp3._
import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, when}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class DmpAuthenticationSpec extends FunSuite with BeforeAndAfterAll {

  private var httpClient: OkHttpClient = null
  private var remoteCall: Call = null
  private var headers: java.util.Map[String, String] = null
  private var httpUrl: HttpUrl = null
  private val hadoopAccount = "spark"
  private val password = "123456"

  private var dmpAuthentication: DmpAuthentication = null

  override def beforeAll(): Unit = {
    httpClient = mock(classOf[OkHttpClient])
    HttpUtils.mockHttpClient = httpClient
    dmpAuthentication = new DmpAuthentication("token", "localhost")
    remoteCall = mock(classOf[Call])
    headers = mock(classOf[java.util.Map[String, String]])
    httpUrl = new HttpUrl.Builder()
      .scheme("http")
      .host("localhost")
      .encodedPath("/url.com")
      .build()
  }

  test(s"dmp auth get hadoop account password") {
    val responseBody =
      """{"code":200,"message":"SUCCESS","data":"123456"}"""
    val request = new Request.Builder()
      .method("GET", null)
      .url(httpUrl)
      .build()

    when(httpClient.newCall(any())).thenReturn(remoteCall)
    when(remoteCall.execute()).thenReturn(mockResponse(request, responseBody))
    assert(dmpAuthentication.getPassword(hadoopAccount).equals(password))
  }

  test(s"dmp auth validate hadoop account and password") {
    val requestBody =
      """{"account":"spark","password":"123456"}"""
    val responseBody =
      """{"code":200,"message":"SUCCESS","data":true}"""
    val request = new Request.Builder()
      .method("POST", RequestBody.create(
        requestBody,
        MediaType.parse("application/json")))
      .url(httpUrl)
      .build()

    when(httpClient.newCall(any())).thenReturn(remoteCall)
    when(remoteCall.execute()).thenReturn(mockResponse(request, responseBody))
    assert(dmpAuthentication.validate(hadoopAccount, password))
  }

  test("should not throw exception while encountering unknown properties") {
    val request1 = new Request.Builder()
      .method("GET", null)
      .url(httpUrl)
      .build()
    val responseBody1 =
      """{"code":200,"message":"SUCCESS","data":"123456","otherMsg": "request success"}"""

    when(httpClient.newCall(any())).thenReturn(remoteCall)
    when(remoteCall.execute()).thenReturn(mockResponse(request1, responseBody1))
    assert(dmpAuthentication.getPassword(hadoopAccount).equals(password))

    val requestBody =
      """{"account":"spark","password":"123456"}"""
    val request2 = new Request.Builder()
      .method("POST", RequestBody.create(
        requestBody,
        MediaType.parse("application/json")))
      .url(httpUrl)
      .build()
    val responseBody2 =
      """{"code":200,"message":"SUCCESS","data":true, "otherMsg": "request success"}"""

    when(httpClient.newCall(any())).thenReturn(remoteCall)
    when(remoteCall.execute()).thenReturn(mockResponse(request2, responseBody2))
    assert(dmpAuthentication.validate(hadoopAccount, password))
  }

  private def mockResponse(request: Request, responseBody: String): Response = {
    new Response.Builder()
      .request(request)
      .protocol(Protocol.HTTP_1_1)
      .code(200).message("")
      .body(ResponseBody.create(
        responseBody,
        MediaType.parse("application/json")))
      .build()
  }
}
