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

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.{Failure, Success, Try}

import com.shopee.livy.HBOProcessor.{HBO_ENABLED, HBO_URL}
import com.shopee.livy.utils.HttpUtils
import okhttp3.HttpUrl

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, Logging}

object HBOProcessor {
  val HBO_ENABLED = "spark.livy.hbo.enabled"
  val HBO_URL = "livy.rsc.spark.hbo.url"
}

class HBOProcessor extends ApplicationEnvProcessor with Logging {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    val appConf = applicationEnvContext.appConf

    val enabled = appConf.getOrDefault(HBO_ENABLED, "false")
    if ("true".equalsIgnoreCase(enabled)) {

      val hboUrl = appConf.get(HBO_URL)
      val userTags = applicationEnvContext.userYarnTags.getOrElse("")
      val resp: Try[Map[String, AnyRef]] = getHBOConfig(hboUrl, userTags)

      resp match {
        case Success(hbo) =>
          info(s"HBO config $hbo")
          applicationEnvContext.optimizedConf.foreach(c => c.asScala.put("hbo", hbo))
          hbo.foreach(kv => {
            kv._2 match {
              case hboData: List[AnyRef] => hboData.foreach(x => {
                val ruleEntry = x.asInstanceOf[Map[String, AnyRef]]
                val conf = ruleEntry("conf").asInstanceOf[Map[String, String]]
                conf.foreach(c => {
                  val key = c._1
                  val value = c._2
                  applicationEnvContext.appConf.asScala.put(key, value)
                  info(s"Setting up the HBO config in Spark $key=$value")
                })
              })
              case _ =>
            }
          })
        case Failure(exception) =>
          error("Fail to get HBO config.", exception)
      }
    }
  }

  def getHBOConfig(hboUrl: String, userTags: String): Try[Map[String, AnyRef]] = {
    val headers: Map[String, String] = Map("Content-type" -> "application/json")
    val httpUrl = HttpUrl.parse(hboUrl)
      .newBuilder()
      .encodedPath("/api/v1/hbo")
      .addQueryParameter("yarnTags", userTags)
      .build()
    /**
     * hbo status:
     *  400: bad request; 404: not found; 500: internal server error
     */
    HttpUtils.doGet[Map[String, AnyRef]](httpUrl, headers, Some(Set[Int](400, 404, 500)))
  }
}
