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

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.shopee.livy.SdiYarnAmEnvProcessor._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.livy.ApplicationEnvContext

class SdiYarnAmEnvProcessorSpec extends FunSuite with BeforeAndAfterAll {

  private val appConf = new mutable.HashMap[String, String]

  override def beforeAll(): Unit = {
    appConf += (
      sdiEnvPrefix + "PYSPARK_DRIVER_PYTHON" -> "/usr/share/python3",
      sdiEnvPrefix + "PYSPARK_PYTHON" -> "/usr/share/python",
      sdiEnvPrefix + "TEST_ENV" -> "amVal",
      sdiEnvPrefix + "rssEnabled" -> "true"
    )
  }

  override def afterAll(): Unit = {
    appConf.clear()
  }

  test("should pass environment variables for the AM to the appConf.") {
    val context = ApplicationEnvContext(new util.HashMap[String, String](), appConf.asJava)
    val processor = new SdiYarnAmEnvProcessor()
    processor.process(context)

    assert(appConf.contains(amEnvPrefix + "PYSPARK_DRIVER_PYTHON"))
    assert(appConf.contains(amEnvPrefix + "PYSPARK_PYTHON"))
    assert(appConf.contains(amEnvPrefix + "TEST_ENV"))
    assert(appConf.contains(amEnvPrefix + "rssEnabled"))
    assert(appConf(amEnvPrefix + "PYSPARK_DRIVER_PYTHON") == "/usr/share/python3")
    assert(appConf(amEnvPrefix + "PYSPARK_PYTHON") == "/usr/share/python")
    assert(appConf(amEnvPrefix + "TEST_ENV") == "amVal")
    assert(appConf(amEnvPrefix + "rssEnabled") == "true")
  }
}
