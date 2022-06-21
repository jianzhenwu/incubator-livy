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

import com.shopee.livy.HudiConfProcessor.SPARK_LIVY_HUDI_JAR
import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import org.apache.livy.ApplicationEnvContext
import org.apache.livy.ApplicationEnvProcessor.SPARK_AUX_JAR

class HudiConfProcessorSpec extends ScalatraSuite with FunSpecLike {

  describe("HudiConfProcessor") {

    it("should overwrite hudi jar") {
      val appConf = mutable.HashMap[String, String](
        SPARK_LIVY_HUDI_JAR -> "/path/hudi.jar")
      val context = ApplicationEnvContext(new util.HashMap[String, String](),
        appConf.asJava)
      val processor = new HudiConfProcessor()
      processor.process(context)

      assert(appConf(SPARK_AUX_JAR) == "/path/hudi.jar")
    }

    it("should not overwrite hudi jar with empty value") {
      val appConf = mutable.HashMap[String, String](
        SPARK_LIVY_HUDI_JAR -> "")
      val context = ApplicationEnvContext(new util.HashMap[String, String](),
        appConf.asJava)
      val processor = new HudiConfProcessor()
      processor.process(context)
      assert(!appConf.contains(SPARK_AUX_JAR))
    }

    it("should merge hudi jar") {
      val appConf = mutable.HashMap[String, String](
        SPARK_LIVY_HUDI_JAR -> "/path/hudi.jar",
        SPARK_AUX_JAR -> "/path/others.jar")
      val context = ApplicationEnvContext(new util.HashMap[String, String](),
        appConf.asJava)
      val processor = new HudiConfProcessor()
      processor.process(context)

      assert(appConf(SPARK_AUX_JAR) == "/path/hudi.jar,/path/others.jar")
    }
  }
}
