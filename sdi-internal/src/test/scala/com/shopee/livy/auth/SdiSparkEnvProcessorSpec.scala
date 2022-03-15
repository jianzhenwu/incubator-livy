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

package com.shopee.livy.auth

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.shopee.livy.{S3aEnvProcessor, SdiHadoopEnvProcessor}
import org.mockito.Mockito.{mock, when}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, ClassLoaderUtils}
import org.apache.livy.client.common.ClientConf

class SdiSparkEnvProcessorSpec extends FunSuite with BeforeAndAfterAll {

  test("Test SdiSparkEnvProcessor") {

    val mockDmp: DmpAuthentication = mock(classOf[DmpAuthentication])
    when(mockDmp.getPassword("spark")).thenReturn("123456")

    SdiHadoopEnvProcessor.mockDmpAuthentication = mockDmp

    val url = ClassLoaderUtils.getContextOrDefaultClassLoader
      .getResource("spark-conf")

    val env = mutable.HashMap[String, String](
      "SPARK_CONF_DIR" -> url.getPath
    )

    val appConf = mutable.HashMap[String, String](
      "spark.executor.cores" -> "1",
      "spark.dynamicAllocation.maxExecutors" -> "50",
      "spark.dynamicAllocation.enabled" -> "true",
      "spark.driver.memoryOverhead" -> "100M",
      "spark.executor.memoryOverhead" -> "100M",
      S3aEnvProcessor.SPARK_S3A_ENABLED -> "true",
      ClientConf.LIVY_APPLICATION_HADOOP_USER_NAME_KEY -> "spark")

    val context = ApplicationEnvContext(env.asJava, appConf.asJava)

    val processor =
      ApplicationEnvProcessor.apply("com.shopee.livy.SdiSparkEnvProcessor")
    processor.process(context)

    // username and password should be in env.
    assert(env("HADOOP_USER_NAME") == "spark")
    assert(env("HADOOP_USER_RPCPASSWORD") == "123456")

    // aws package should be in classpath when spark.s3a.enabled
    assert(env("HADOOP_CLASSPATH") ==
      "$HADOOP_CLASSPATH:/hadoop/share/hadoop/tools/lib/hadoop-aws-*.jar:" +
        "/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar")

    // should optimized spark conf
    assert(appConf("spark.executor.cores") == "1")
    assert(appConf("spark.dynamicAllocation.enabled") == "true")
    assert(appConf("spark.sql.shuffle.partitions").toInt == 50)
    assert(appConf("spark.default.parallelism").toInt == 50)
    assert(appConf("spark.dynamicAllocation.maxExecutors").toInt == 50)
    assert(appConf("spark.driver.memoryOverhead") == "1G")
    assert(appConf("spark.executor.memoryOverhead") == "1G")
  }

}
