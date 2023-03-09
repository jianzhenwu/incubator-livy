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

import scala.collection.JavaConverters.mutableMapAsJavaMapConverter
import scala.collection.mutable

import com.shopee.livy.AlluxioConfProcessor.{SPARK_LIVY_ALLUXIO_ARCHIVE, SPARK_LIVY_ALLUXIO_ENV_ENABLED}
import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import org.apache.livy.ApplicationEnvContext

class AlluxioConfProcessorSpec extends ScalatraSuite with FunSpecLike {
  describe("AlluxioConfProcessor") {

    it("should contain alluxio package in spark archive") {
      val appConf = mutable.HashMap[String, String](
        SPARK_LIVY_ALLUXIO_ENV_ENABLED -> "true",
        SPARK_LIVY_ALLUXIO_ARCHIVE -> "/user/alluxio-archive.zip")
      val context = ApplicationEnvContext(new util.HashMap[String, String](),
        appConf.asJava)
      val processor = new AlluxioConfProcessor()
      processor.process(context)

      assert(appConf("spark.archives") == "/user/alluxio-archive.zip")
    }
  }
}
