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

import com.shopee.livy.IpynbEnvProcessor.{HADOOP_USER_NAME, HADOOP_USER_RPCPASSWORD, SPARK_LIVY_IPYNB_ARCHIVES, SPARK_LIVY_IPYNB_ENV_ENABLED, SPARK_LIVY_IPYNB_FILES, SPARK_LIVY_IPYNB_JARS, SPARK_LIVY_IPYNB_PY_FILES}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, when}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike}
import org.scalatra.test.scalatest.ScalatraSuite

import org.apache.livy.ApplicationEnvContext
import org.apache.livy.LivyConf.{SPARK_ARCHIVES, SPARK_FILES, SPARK_JARS, SPARK_PY_FILES}

class IpynbEnvProcessorSpec extends ScalatraSuite
  with FunSpecLike
  with BeforeAndAfterAll {

  val hadoopUser = "John"
  val hadoopPassword = "123456"
  val pyFiles = "/usr/share/spark/python/lib/pyspark.zip,/usr/share/spark/python/lib/py4j-*.zip"
  val env: mutable.Map[String, String] = mutable.HashMap[String, String](
    HADOOP_USER_NAME -> hadoopUser,
    HADOOP_USER_RPCPASSWORD -> hadoopPassword,
    "SPARK_HOME" -> "/usr/share/spark"
  )

  var processor: IpynbEnvProcessor = _

  override def beforeAll: Unit = {
    IpynbEnvProcessor.mockFileSystem = Some(mock(classOf[FileSystem]))
    processor = new IpynbEnvProcessor()
    val fs = IpynbEnvProcessor.mockFileSystem.get
    val jarsStatus: Array[FileStatus] = Array(
      new FileStatus(0, false, 0, 0, 0, new Path("s3a://bucket_a/jars/main.jar"))
    )
    val filesStatus: Array[FileStatus] = Array(
      new FileStatus(0, false, 0, 0, 0, new Path("s3a://bucket_a/files/log4j.properties"))
    )
    val archivesStatus: Array[FileStatus] = Array(
      new FileStatus(0, false, 0, 0, 0, new Path("s3a://bucket_a/archives/module.zip"))
    )
    val pyFilesStatus: Array[FileStatus] = Array(
      new FileStatus(0, false, 0, 0, 0, new Path("s3a://bucket_a/pyFiles/module.zip"))
    )
    when(fs.listStatus(new Path("s3a://bucket_a/jars/"))).thenReturn(jarsStatus)
    when(fs.listStatus(new Path("s3a://bucket_a/files/"))).thenReturn(filesStatus)
    when(fs.listStatus(new Path("s3a://bucket_a/archives/"))).thenReturn(archivesStatus)
    when(fs.listStatus(new Path("s3a://bucket_a/pyFiles/"))).thenReturn(pyFilesStatus)
    when(fs.exists(any(classOf[Path]))).thenReturn(true)

    val filesStatusBucketB: Array[FileStatus] = Array(
      new FileStatus(0, false, 0, 0, 0, new Path("s3a://bucket_b/files/log4j.properties"))
    )
    val archivesStatusBucketC: Array[FileStatus] = Array(
      new FileStatus(0, false, 0, 0, 0, new Path("s3a://bucket_c/archives/module.zip"))
    )
    val pyFilesStatusBucketD: Array[FileStatus] = Array(
      new FileStatus(0, false, 0, 0, 0, new Path("s3a://bucket_d/pyFiles/module.zip"))
    )
    when(fs.listStatus(new Path("s3a://bucket_b/files/"))).thenReturn(filesStatusBucketB)
    when(fs.listStatus(new Path("s3a://bucket_c/archives/"))).thenReturn(archivesStatusBucketC)
    when(fs.listStatus(new Path("s3a://bucket_d/pyFiles/"))).thenReturn(pyFilesStatusBucketD)
  }

  override def afterAll: Unit = {
    IpynbEnvProcessor.mockFileSystem = None
  }

  describe("IpynbEnvProcessor") {
    it("should skip when spark.livy.ipynb.env.enabled is false") {
      val appConf = mutable.HashMap[String, String](
        SPARK_LIVY_IPYNB_ENV_ENABLED -> "false"
      )
      val context = ApplicationEnvContext(env.asJava, appConf.asJava)
      processor.process(context)
      appConf should not contain (SPARK_PY_FILES -> pyFiles)
    }

    it("should work when there are no dependencies") {
      val appConf = mutable.HashMap[String, String](
        SPARK_LIVY_IPYNB_ENV_ENABLED -> "true"
      )
      val context = ApplicationEnvContext(env.asJava, appConf.asJava)
      processor.process(context)
      appConf(SPARK_PY_FILES) should include (pyFiles)
    }

    it("should work when dependency is empty") {
      val appConf = mutable.HashMap[String, String](
        SPARK_LIVY_IPYNB_ENV_ENABLED -> "true",
        SPARK_LIVY_IPYNB_JARS -> ""
      )
      val context = ApplicationEnvContext(env.asJava, appConf.asJava)
      processor.process(context)
      appConf(SPARK_PY_FILES) should include (pyFiles)
    }

    it("should work when there is a bucket") {
      val appConf = mutable.HashMap[String, String](
        SPARK_LIVY_IPYNB_ENV_ENABLED -> "true",
        SPARK_LIVY_IPYNB_JARS -> "s3a://bucket_a/jars/*.jar",
        SPARK_LIVY_IPYNB_FILES -> "s3a://bucket_a/files/*",
        SPARK_LIVY_IPYNB_ARCHIVES -> "s3a://bucket_a/archives/*",
        SPARK_LIVY_IPYNB_PY_FILES -> "s3a://bucket_a/pyFiles/*"
      )
      val context = ApplicationEnvContext(env.asJava, appConf.asJava)
      processor.process(context)
      appConf(SPARK_JARS) should include("s3a://bucket_a/jars/main.jar")
      appConf(SPARK_FILES) should include("s3a://bucket_a/files/log4j.properties")
      appConf(SPARK_ARCHIVES) should include("s3a://bucket_a/archives/module.zip")
      appConf(SPARK_PY_FILES) should include("s3a://bucket_a/pyFiles/module.zip")
      appConf("spark.hadoop.fs.s3a.bucket.bucket_a.access.key") should be(hadoopUser)
      appConf("spark.hadoop.fs.s3a.bucket.bucket_a.secret.key") should be(hadoopPassword)
      appConf(SPARK_PY_FILES) should include (pyFiles)
    }

    it("should work when there are multiple buckets") {
      val appConf = mutable.HashMap[String, String](
        SPARK_LIVY_IPYNB_ENV_ENABLED -> "true",
        SPARK_JARS -> "spark.jar",
        SPARK_LIVY_IPYNB_JARS -> "s3a://bucket_a/jars/*.jar",
        SPARK_LIVY_IPYNB_FILES -> "s3a://bucket_b/files/*",
        SPARK_LIVY_IPYNB_ARCHIVES -> "s3a://bucket_c/archives/*",
        SPARK_LIVY_IPYNB_PY_FILES -> "s3a://bucket_d/pyFiles/*"
      )
      val context = ApplicationEnvContext(env.asJava, appConf.asJava)
      processor.process(context)
      appConf(SPARK_JARS) should be ("s3a://bucket_a/jars/main.jar,spark.jar")
      appConf(SPARK_FILES) should include ("s3a://bucket_b/files/log4j.properties")
      appConf(SPARK_ARCHIVES) should include ("s3a://bucket_c/archives/module.zip")
      appConf(SPARK_PY_FILES) should include ("s3a://bucket_d/pyFiles/module.zip")
      Seq("bucket_a", "bucket_b", "bucket_c", "bucket_d").foreach { bucket =>
        appConf(s"spark.hadoop.fs.s3a.bucket.$bucket.access.key") should be(hadoopUser)
        appConf(s"spark.hadoop.fs.s3a.bucket.$bucket.secret.key") should be(hadoopPassword)
      }
      appConf(SPARK_PY_FILES) should include (pyFiles)
    }
  }
}
