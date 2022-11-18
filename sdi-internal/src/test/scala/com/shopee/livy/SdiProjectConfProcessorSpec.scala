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

import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.shopee.livy.SdiProjectConfProcessor._
import com.shopee.livy.auth.DmpAuthentication
import org.mockito.Matchers.anyString
import org.mockito.Mockito.{mock, when}
import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import org.apache.livy.ApplicationEnvContext
import org.apache.livy.client.common.ClientConf

class SdiProjectConfProcessorSpec extends ScalatraSuite
  with FunSpecLike {

  private var processor: SdiProjectConfProcessor = _

  override def beforeAll(): Unit = {
    mockDmpAuthentication = mock(classOf[DmpAuthentication])
    processor = new SdiProjectConfProcessor()
  }

  describe("SdiProjectConfProcessorSpec") {

    it("should put password when request user is project") {
      when(mockDmpAuthentication.getPassword(anyString())).thenReturn("password")
      when(mockDmpAuthentication.belongProject(anyString(), anyString())).thenReturn(true)

      val appConf = mutable.Map[String, String](
        SPARK_SHOPEE_DATA_INFRA_PROJECT -> "livy",
        ClientConf.LIVY_APPLICATION_HADOOP_USER_NAME_KEY -> "livy"
      )

      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_SHOPEE_DATA_INFRA_PROJECT_PASSWORD) should be ("password")
    }

    it("should put project password when request user is not project") {
      when(mockDmpAuthentication.getPassword(anyString())).thenReturn("password")
      when(mockDmpAuthentication.belongProject(anyString(), anyString())).thenReturn(true)

      val appConf = mutable.Map[String, String](
        SPARK_SHOPEE_DATA_INFRA_PROJECT -> "livy",
        ClientConf.LIVY_APPLICATION_HADOOP_USER_NAME_KEY -> "user"
      )

      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_SHOPEE_DATA_INFRA_PROJECT_PASSWORD) should be ("password")
    }

    it("should not put project password when request user does not belong to project") {
      when(mockDmpAuthentication.belongProject(anyString(), anyString())).thenReturn(false)

      val appConf = mutable.Map[String, String](
        SPARK_SHOPEE_DATA_INFRA_PROJECT -> "livy",
        ClientConf.LIVY_APPLICATION_HADOOP_USER_NAME_KEY -> "user"
      )

      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf.keys should not contain SPARK_SHOPEE_DATA_INFRA_PROJECT_PASSWORD
    }

    it("should not put project password when request does not contain project") {
      val appConf = mutable.Map[String, String](
        ClientConf.LIVY_APPLICATION_HADOOP_USER_NAME_KEY -> "user"
      )

      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf.keys should not contain SPARK_SHOPEE_DATA_INFRA_PROJECT_PASSWORD
    }
  }
}
