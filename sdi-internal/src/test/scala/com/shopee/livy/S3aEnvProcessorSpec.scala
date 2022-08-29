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

import java.nio.file.Files
import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters.mutableMapAsJavaMapConverter
import scala.collection.mutable

import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import org.apache.livy.{ApplicationEnvContext, ClassLoaderUtils}

class S3aEnvProcessorSpec  extends ScalatraSuite with FunSpecLike {

  describe("S3aEnvProcessorSpec") {

    it("should import s3a package when s3a enabled") {

      val sparks = Array("/opt/spark-3.2.1-sdi-006-bin-3.3.sdi-011",
        "/opt/spark-3.1.1-sdi-006-bin-3.3.sdi-011",
        "/opt/spark-3.0.2-sdi-022-bin-3.2.0-sdi-001",
        "/opt/spark-2.4.7-sdi-026-bin-2.10.sdi-008")

      sparks.foreach(spark => {
        val env = new JHashMap[String, String]()
        env.put("SPARK_HOME", spark)
        val appConf = new JHashMap[String, String]()
        appConf.put(S3aEnvProcessor.SPARK_LIVY_S3A_ENABLED, "true")

        val ivyPath = Files.createTempDirectory("livy_ivy")
        appConf.put("spark.jars.ivy", ivyPath.toString)

        val processor = new S3aEnvProcessor()
        val context = ApplicationEnvContext(env, appConf)
        processor.process(context)

        assert(appConf.get(S3aEnvProcessor.S3A_PATH_STYLE_ACCESS) == "true")
        Array("hadoop-aws", "java-sdk-bundle").foreach { e =>
          assert(appConf.get("spark.jars").contains(e))
          assert(env.get("SPARK_DIST_CLASSPATH").contains(e))
        }
      })
    }

  }
}
