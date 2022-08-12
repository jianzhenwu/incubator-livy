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

import com.shopee.livy.IpynbConfProcessor.{HADOOP_USER_NAME, HADOOP_USER_RPCPASSWORD, SPARK_LIVY_IPYNB_ARCHIVES, SPARK_LIVY_IPYNB_FILES, SPARK_LIVY_IPYNB_JARS}
import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import org.apache.livy.ApplicationEnvContext
import org.apache.livy.LivyConf.{SPARK_ARCHIVES, SPARK_FILES, SPARK_JARS}

class IpynbConfProcessorSpec extends ScalatraSuite with FunSpecLike {

  val hadoopUser = "John"
  val hadoopPassword = "123456"
  val env: mutable.Map[String, String] = mutable.HashMap[String, String](
    HADOOP_USER_NAME -> hadoopUser,
    HADOOP_USER_RPCPASSWORD -> hadoopPassword
  )

  describe("IpynbConfProcessor") {
    it("should work when there are no dependencies") {
      val appConf = mutable.HashMap[String, String](
      )
      val context = ApplicationEnvContext(env.asJava, appConf.asJava)
      val processor = new IpynbConfProcessor()
      processor.process(context)
      appConf shouldBe empty
    }

    it("should work when dependency is empty") {
      val appConf = mutable.HashMap[String, String](
        SPARK_LIVY_IPYNB_JARS -> ""
      )
      val context = ApplicationEnvContext(env.asJava, appConf.asJava)
      val processor = new IpynbConfProcessor()
      processor.process(context)
      appConf should have size 1
    }

    it("should work when there is a bucket") {
      val appConf = mutable.HashMap[String, String](
        SPARK_LIVY_IPYNB_JARS -> "s3a://bucket_a/jars/*.jar",
        SPARK_LIVY_IPYNB_FILES -> "s3a://bucket_a/files/*",
        SPARK_LIVY_IPYNB_ARCHIVES -> "s3a://bucket_a/archives/*"
      )
      val context = ApplicationEnvContext(env.asJava, appConf.asJava)
      val processor = new IpynbConfProcessor()
      processor.process(context)
      appConf(SPARK_JARS) should include("s3a://bucket_a/jars/*.jar")
      appConf(SPARK_FILES) should include("s3a://bucket_a/files/*")
      appConf(SPARK_ARCHIVES) should include("s3a://bucket_a/archives/*")
      appConf("spark.hadoop.fs.s3a.bucket.bucket_a.access.key") should be(hadoopUser)
      appConf("spark.hadoop.fs.s3a.bucket.bucket_a.secret.key") should be(hadoopPassword)
    }

    it("should work when there are multiple buckets") {
      val appConf = mutable.HashMap[String, String](
        SPARK_JARS -> "spark.jar",
        SPARK_LIVY_IPYNB_JARS -> "s3a://bucket_a/jars/*.jar",
        SPARK_LIVY_IPYNB_FILES -> "s3a://bucket_b/files/*",
        SPARK_LIVY_IPYNB_ARCHIVES -> "s3a://bucket_c/archives/*"
      )
      val context = ApplicationEnvContext(env.asJava, appConf.asJava)
      val processor = new IpynbConfProcessor()
      processor.process(context)
      appConf(SPARK_JARS) should be ("spark.jar,s3a://bucket_a/jars/*.jar")
      appConf(SPARK_FILES) should include ("s3a://bucket_b/files/*")
      appConf(SPARK_ARCHIVES) should include ("s3a://bucket_c/archives/*")
      Seq("bucket_a", "bucket_b", "bucket_c").foreach { bucket =>
        appConf(s"spark.hadoop.fs.s3a.bucket.$bucket.access.key") should be(hadoopUser)
        appConf(s"spark.hadoop.fs.s3a.bucket.$bucket.secret.key") should be(hadoopPassword)
      }
    }
  }
}
