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

import com.shopee.livy.HudiConfProcessor._
import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import org.apache.livy.{ApplicationEnvContext, LivyConf}

class HudiConfProcessorSpec extends ScalatraSuite with FunSpecLike {

  describe("HudiConfProcessor") {

    it("should overwrite hudi jar") {
      val appConf = mutable.HashMap[String, String](
        SPARK_LIVY_HUDI_JAR -> "/user/spark-hudi-bundle.jar")
      val context = ApplicationEnvContext(new util.HashMap[String, String](),
        appConf.asJava)
      val processor = new HudiConfProcessor()
      processor.process(context)

      assert(appConf(SPARK_AUX_JAR) == "/user/spark-hudi-bundle.jar")
    }

    it("should overwrite hudi jar with empty value") {
      val appConf = mutable.HashMap[String, String](
        SPARK_LIVY_HUDI_JAR -> "")
      val context = ApplicationEnvContext(new util.HashMap[String, String](),
        appConf.asJava)
      val processor = new HudiConfProcessor()
      processor.process(context)
      assert(appConf.contains(SPARK_AUX_JAR))
    }

    it("should merge hudi jar") {
      val appConf = mutable.HashMap[String, String](
        SPARK_LIVY_HUDI_JAR -> "/user/spark-hudi-bundle.jar",
        SPARK_AUX_JAR -> "/default/spark-hudi-bundle.jar,/default/others.jar")
      val context = ApplicationEnvContext(new util.HashMap[String, String](),
        appConf.asJava)
      val processor = new HudiConfProcessor()
      processor.process(context)

      appConf(SPARK_AUX_JAR) should include("/user/spark-hudi-bundle.jar")
      appConf(SPARK_AUX_JAR) should include("/default/others.jar")
      appConf(SPARK_AUX_JAR) should not include("/default/spark-hudi-bundle.jar")
    }

    it("should add hudi catalog config key to appConf") {
      val appConf = mutable.HashMap[String, String](
        SPARK_SQL_EXTENSIONS -> SPARK_HUDI_EXTENSION_CLASS_NAME,
        LivyConf.SPARK_FEATURE_VERSION -> "3.2")
      val context = ApplicationEnvContext(new util.HashMap[String, String](), appConf.asJava)
      val processor = new HudiConfProcessor()
      processor.process(context)

      appConf("spark.sql.catalog.spark_catalog") should be (SPARK_HUDI_CATALOG_CLASS_NAME)
    }

    it("should not add hudi catalog config key") {
      val appConf = mutable.HashMap[String, String](
        SPARK_SQL_EXTENSIONS -> SPARK_HUDI_EXTENSION_CLASS_NAME,
        LivyConf.SPARK_FEATURE_VERSION -> "3.1")
      val context = ApplicationEnvContext(new util.HashMap[String, String](), appConf.asJava)
      val processor = new HudiConfProcessor()
      processor.process(context)

      appConf should not contain key ("spark.sql.catalog.spark_catalog")
    }
  }
}
