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

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.shopee.livy.auth.DmpAuthentication._
import com.shopee.livy.utils.HttpUtils
import okhttp3.{HttpUrl, MediaType, RequestBody}

import org.apache.livy.Logging

/**
 * DMP Hadoop account authentication for Livy.
 */
class DmpAuthentication(serverToken: String, serverHost: String) extends Logging {

  private val objectMapper: ObjectMapper = new ObjectMapper()
    .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  private val headers: Map[String, String] =
    Map("Content-type" -> "application/json", "X-DMP-Authorization" -> serverToken)

  def getPassword(hadoopAccount: String): String = {
    require(hadoopAccount != null, s"Hadoop account $hadoopAccount must exist")

    val url = new HttpUrl.Builder()
      .scheme("https")
      .host(serverHost)
      .encodedPath(s"$HADOOP_ACCOUNT_PASSWORD_PATH$hadoopAccount")
      .build()
    val response: Try[HadoopPasswordResponse] =
      HttpUtils.doGet[HadoopPasswordResponse](url, headers)

    handleResponse[String](response)(response.get.data)
  }

  def validate(hadoopAccount: String, password: String): Boolean = {
    val userInfo = HadoopAccount(hadoopAccount, password)
    val url = new HttpUrl.Builder()
      .scheme("https")
      .host(serverHost)
      .encodedPath(s"$HADOOP_ACCOUNT_VALIDATE_PATH")
      .build()
    val body = RequestBody.create(
      objectMapper.writeValueAsString(userInfo),
      MediaType.parse("application/json"))
    val response: Try[ValidateResponse] =
      HttpUtils.doPost[ValidateResponse](url, headers, body)

    handleResponse[Boolean](response)(response.get.data)
  }

  /**
   * Returns whether the hadoop account belong to the project.
   */
  def belongProject(hadoopAccount: String, projectCode: String): Boolean = {
    val url = new HttpUrl.Builder()
      .scheme("https")
      .host(serverHost)
      .encodedPath(s"$HADOOP_ACCOUNT_ALL_PROJECTS_PATH")
      .addQueryParameter("hadoopAccount", hadoopAccount)
      .build()
    val response: Try[AllProjectsResponse] =
      HttpUtils.doGet[AllProjectsResponse](url, headers)

    handleResponse[Boolean](response) {
      response.get.data.exists(_.projectCode.equalsIgnoreCase(projectCode))
    }
  }

  /**
   * Returns a map of project code to [[AdditionalProperties]].
   */
  def getAdditionalProperties(projectCodes: Seq[String]): Map[String, AdditionalProperties] = {
    val body = RequestBody.create(
      objectMapper.writeValueAsString(projectCodes),
      MediaType.parse("application/json"))
    val url = new HttpUrl.Builder()
      .scheme("https")
      .host(serverHost)
      .encodedPath(s"$PROJECT_ACCOUNT_PASSWORD_PATH")
      .build()

    val response: Try[ProjectPasswordResponse] =
      HttpUtils.doPost[ProjectPasswordResponse](url, headers, body)

    handleResponse[Map[String, AdditionalProperties]](response) {
      response.get.data
    }
  }

  private def handleResponse[T: ClassTag](response: Try[Any])(f: => T): T = {
    response match {
      case Success(_) => f
      case Failure(exception) =>
        error("Internal authentication server error", exception)
        throw new AuthClientException(s"Internal authentication server error", exception)
    }
  }
}

object DmpAuthentication {

  private val SERVER_TOKEN = "LIVY_SERVER_AUTH_SERVER_TOKEN"

  private val SERVER_HOST = "LIVY_SERVER_AUTH_SERVER_HOST"

  private val HADOOP_ACCOUNT_PASSWORD_PATH =
    "/ram/api/v1/developer/sensitive/hadoopAccount/pwd/"

  private val HADOOP_ACCOUNT_VALIDATE_PATH =
    "/ram/api/v1/developer/sensitive/hadoopAccount/validate"

  private val PROJECT_ACCOUNT_PASSWORD_PATH =
    "/ram/api/v1/developer/sensitive/projectAccount/pwd"

  private val HADOOP_ACCOUNT_ALL_PROJECTS_PATH =
    "/ram/api/v1/developer/user/identity/hadoop/account/allProjects"

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

case class HadoopPasswordResponse(code: Int, message: String, data: String)

case class ValidateResponse(code: Int, message: String, data: Boolean)

case class AllProjectsResponse(code: Int, message: String, data: List[ProjectInfo])

case class ProjectPasswordResponse(
    code: Int,
    message: String,
    data: Map[String, AdditionalProperties])

case class ProjectInfo(
    identityName: String,
    projectCode: String,
    hadoopAccount: String,
    email: String)

case class AdditionalProperties(
    projectCode: String,
    prodServiceAccount: String,
    prodServicePassword: String,
    stagServiceAccount: String,
    stagServicePassword: String)

class AuthClientException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)
