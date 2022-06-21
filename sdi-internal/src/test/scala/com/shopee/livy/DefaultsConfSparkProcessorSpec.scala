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

import scala.collection.JavaConverters.mutableMapAsJavaMapConverter
import scala.collection.mutable

import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import org.apache.livy.{ApplicationEnvContext, ClassLoaderUtils, SessionType}

class DefaultsConfSparkProcessorSpec extends ScalatraSuite
  with FunSpecLike {

  describe("DefaultsConfSparkProcessor") {
    it("should contain correct spark conf when using batches") {

      val url = ClassLoaderUtils.getContextOrDefaultClassLoader
        .getResource("spark-conf")

      val env = mutable.HashMap[String, String](
        "SPARK_CONF_DIR" -> url.getPath
      )

      val appConf = mutable.HashMap[String, String](
        "spark.driver.extraClassPath" -> "/user"
      )
      val context = ApplicationEnvContext(env.asJava, appConf.asJava,
        Some(SessionType.Batches))
      val processor = new DefaultsConfSparkProcessor()
      processor.process(context)

      assert(appConf("spark.driver.extraClassPath") == "/default:/user")

      Array("spark.driver.extraLibraryPath",
        "spark.driver.defaultJavaOptions",
        "spark.driver.extraJavaOptions").foreach( e =>
        assert(!appConf.contains(e))
      )
    }

    it("should contain correct spark conf when using interactive") {

      val url = ClassLoaderUtils.getContextOrDefaultClassLoader
        .getResource("spark-conf")

      val env = mutable.HashMap[String, String](
        "SPARK_CONF_DIR" -> url.getPath
      )

      val appConf = mutable.HashMap[String, String](
        "spark.driver.extraClassPath" -> "/user"
      )
      val context = ApplicationEnvContext(env.asJava, appConf.asJava,
        Some(SessionType.Interactive))
      val processor = new DefaultsConfSparkProcessor()
      processor.process(context)

      assert(appConf("spark.driver.extraClassPath") == "/default:/user")
      assert(appConf("spark.driver.extraLibraryPath") == "/default")

      assert(appConf("spark.driver.defaultJavaOptions") == "-Ddefault")
      assert(appConf("spark.driver.extraJavaOptions") == "-Ddefault")
    }
  }

}
