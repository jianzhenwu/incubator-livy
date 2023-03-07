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

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.Success

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.mockito.Matchers.{anyObject, anyString}
import org.mockito.Mockito.{mock, when}
import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import org.apache.livy.{ApplicationEnvContext, SessionType}

class HBOProcessorSpec extends ScalatraSuite with FunSpecLike {

  val objectMapper: ObjectMapper = new ObjectMapper()
    .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  val hboUrl = "https://percept.data-infra.shopee.io"
  val responseBody: String =
    """
      |{
      |  "status": "true",
      |  "data": [
      |    {
      |      "rule": "cpu",
      |      "conf": {
      |        "spark.executor.cores": "2"
      |      }
      |    },
      |    {
      |      "rule": "rss",
      |      "conf": {
      |        "spark.rss.enabled": "true"
      |      }
      |    },
      |    {
      |      "rule": "memory",
      |      "conf": {
      |        "spark.executor.memory": "4g"
      |      }
      |    }
      |  ]
      |}
      |""".stripMargin

  describe("HBOProcessorSpec") {

    it("should process hbo config") {
      val processor = mock(classOf[HBOProcessor])
      when(processor.getHBOConfig(anyString(), anyString())).thenReturn(
        Success(objectMapper.readValue(responseBody, classOf[Map[String, AnyRef]]))
      )
      when(processor.process(anyObject())).thenCallRealMethod()

      val appConf: util.Map[String, String] = new util.HashMap[String, String]();
      appConf.put(HBOProcessor.HBO_ENABLED, "true")
      appConf.put(HBOProcessor.HBO_URL, hboUrl)
      val optimizedConf = new java.util.HashMap[String, Object]()
      val context = ApplicationEnvContext(new util.HashMap[String, String](),
        appConf, Some(SessionType.Batches), Some(optimizedConf), Some(hboUrl)
      )
      processor.process(context)

      val hboConfig: Map[String, AnyRef] =
        optimizedConf.asScala("hbo").asInstanceOf[Map[String, AnyRef]]

      hboConfig should contain ("status" -> "true")
      val data = hboConfig("data").asInstanceOf[List[AnyRef]]
      data.foreach(x => {
        val ruleEntry = x.asInstanceOf[Map[String, AnyRef]]
        val rule = ruleEntry("rule").asInstanceOf[String]
        val conf = ruleEntry("conf").asInstanceOf[Map[String, String]]
        rule match {
          case "cpu" => conf should contain ("spark.executor.cores" -> "2")
          case "rss" => conf should contain ("spark.rss.enabled" -> "true")
          case "memory" => conf should contain ("spark.executor.memory" -> "4g")
        }
      })
    }
  }
}
