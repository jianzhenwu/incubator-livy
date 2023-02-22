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

package com.shopee.livy.coce

import scala.collection.JavaConverters.setAsJavaSetConverter

import com.ctrip.framework.apollo.Config
import org.mockito.Matchers.{anyObject, anySetOf}
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.FunSpecLike
import org.scalatestplus.mockito.MockitoSugar.mock
import org.scalatra.test.scalatest.ScalatraSuite

import org.apache.livy.LivyConf

class ApolloConfigCenterProviderSpec extends ScalatraSuite with FunSpecLike {

  private var apolloConfigCenterProvider: ApolloConfigCenterProvider = _
  private var livyConf: LivyConf = _

  override def beforeAll(): Unit = {
    val mockConfigCenter = mock[Config]
    ApolloConfigCenterProvider.mockConfigCenter = mockConfigCenter
    livyConf = new LivyConf(false)
    apolloConfigCenterProvider = new ApolloConfigCenterProvider(livyConf)
    when(mockConfigCenter.getPropertyNames).thenReturn(
      Set("livy.server.spark-home.v2", "other.key").asJava
    )

    when(mockConfigCenter.getProperty("livy.server.spark-home.v2", null))
      .thenReturn("/usr/share/spark2")

    when(mockConfigCenter.getProperty("other.key", null))
      .thenReturn("other.value")

    when(mockConfigCenter.addChangeListener(anyObject()))
      .thenAnswer(new Answer[Unit] {
        override def answer(invocation: InvocationOnMock): Unit = {}
      })
  }

  describe("ApolloConfigCenterProviderSpec") {

    it("should contain effective configuration") {
      apolloConfigCenterProvider.start()
      livyConf.get("livy.server.spark-home.v2") should be ("/usr/share/spark2")
      livyConf.toMap should not contain key ("other.key")
    }
  }
}
