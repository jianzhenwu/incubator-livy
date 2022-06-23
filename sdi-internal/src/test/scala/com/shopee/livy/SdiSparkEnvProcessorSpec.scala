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

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.shopee.livy.SparkDatasourceProcessor._
import com.shopee.livy.SparkDatasourceProcessorSpec._
import com.shopee.livy.auth.DmpAuthentication
import com.shopee.livy.HudiConfProcessor.SPARK_LIVY_HUDI_JAR
import org.mockito.Matchers.anyString
import org.mockito.Mockito.{mock, when}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, ClassLoaderUtils, LivyConf, SessionType}
import org.apache.livy.ApplicationEnvProcessor.SPARK_AUX_JAR
import org.apache.livy.client.common.ClientConf

class SdiSparkEnvProcessorSpec extends FunSuite with BeforeAndAfterAll {

  test("Test SdiSparkEnvProcessor") {

    val mockDmp: DmpAuthentication = mock(classOf[DmpAuthentication])
    when(mockDmp.getPassword("spark")).thenReturn("123456")

    SdiHadoopEnvProcessor.mockDmpAuthentication = mockDmp

    val yarnRouterMapping = mock(classOf[YarnRouterMapping])
    YarnRouterMapping.mockYarnRouterMapping = yarnRouterMapping
    when(yarnRouterMapping.getCluster(anyString())).thenReturn("cluster1")

    val url = ClassLoaderUtils.getContextOrDefaultClassLoader
      .getResource("spark-conf")

    val env = mutable.HashMap[String, String](
      "SPARK_CONF_DIR" -> url.getPath,
      "SPARK_HOME" -> "/opt/spark-2.4.7-sdi-026-bin-2.10.sdi-008"
    )

    val appConf = mutable.HashMap[String, String](
      "spark.executor.cores" -> "1",
      "spark.dynamicAllocation.maxExecutors" -> "50",
      "spark.dynamicAllocation.enabled" -> "true",
      "spark.driver.memoryOverhead" -> "100M",
      "spark.executor.memoryOverhead" -> "100M",
      S3aEnvProcessor.SPARK_S3A_ENABLED -> "true",
      DockerEnvProcessor.SPARK_DOCKER_ENABLED -> "true",
      DockerEnvProcessor.SPARK_DOCKER_IMAGE -> "centos7-java-base:v6.0",
      DockerEnvProcessor.RSC_CONF_PREFIX + DockerEnvProcessor.SPARK_DOCKER_MOUNTS ->
        "/usr/share/java/hadoop:/usr/share/java/hadoop:ro",
      StreamingMetricProcessor.STEAMING_METRIC_ENABLED -> "true",
      StreamingMetricProcessor.STRUCTURED_METRIC_ENABLED -> "true",
      StreamingMetricProcessor.RSC_CONF_PREFIX + StreamingMetricProcessor.PUSH_URL ->
        "test_url",
      StreamingMetricProcessor.RSC_CONF_PREFIX + StreamingMetricProcessor.PUSH_TOKEN ->
        "test_token",
      StreamingMetricProcessor.RSC_CONF_PREFIX + StreamingMetricProcessor.PUSH_INTERVAL ->
        "15",
      ClientConf.LIVY_APPLICATION_HADOOP_USER_NAME_KEY -> "spark",
      RssEnvProcessor.SPARK_RSS_ENABLED -> "true",
      RssEnvProcessor.SPARK_YARN_QUEUE -> "queue",
      RssEnvProcessor.RSC_CONF_PREFIX + RssEnvProcessor.YARN_CLUSTER_POLICY_LIST_URL ->
        "http://0.0.0.0/url",
      "livy.rsc.yarn.cluster.cluster1.spark.rss.ha.master.hosts" -> "0.0.0.0",
      "livy.rsc.yarn.cluster.cluster1.spark.rss.master.port" -> "9097",
      "livy.rsc.yarn.cluster.cluster2.spark.rss.ha.master.hosts" -> "0.0.0.1",
      "livy.rsc.yarn.cluster.cluster2.spark.rss.master.port" -> "9098",
      "spark.yarn.appMasterEnv.PYSPARK_PYTHON" -> "./bin/python",
      SPARK_LIVY_HUDI_JAR -> "/path/hudi.jar",
      "spark.driver.extraClassPath" -> "/user")

    val context = ApplicationEnvContext(env.asJava, appConf.asJava,
      Some(SessionType.Batches))
    appConf ++= mutable.Map[String, String](
      SPARK_SQL_CATALOG_HBASE_JARS -> HBASE_JARS,
      SPARK_SQL_CATALOG_HBASE_ENABLED -> "true",
      SPARK_SQL_DATASOURCE_CATALOG_IMPL -> "hive"
    )

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
    assert(appConf("spark.driver.memoryOverhead") == "100M")
    assert(appConf("spark.executor.memoryOverhead") == "100M")

    // docker conf should be in appConf when spark.docker.enabled
    assert(appConf("spark.executorEnv.YARN_CONTAINER_RUNTIME_TYPE") == "docker")
    assert(appConf("spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE") ==
      "centos7-java-base:v6.0")
    assert(appConf("spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS") ==
      "/usr/share/java/hadoop:/usr/share/java/hadoop:ro")
    assert(appConf("spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_TYPE") == "docker")
    assert(appConf("spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE") ==
      "centos7-java-base:v6.0")
    assert(appConf("spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS") ==
      "/usr/share/java/hadoop:/usr/share/java/hadoop:ro")

    // streaming metric conf should be in appConf when spark.streaming.metrics.push.enabled
    assert(appConf("spark.metrics.push.url") == "test_url")
    assert(appConf("spark.metrics.push.token") == "test_token")
    assert(appConf("spark.metrics.send.interval") == "15")
    assert(appConf("spark.streaming.extraListeners") ==
      "org.apache.livy.toolkit.metrics.listener.SparkStreamingListener")
    assert(appConf("spark.sql.streaming.streamingQueryListeners") ==
      "org.apache.livy.toolkit.metrics.listener.StructuredStreamingListener")


    // rss conf should be in appConf when spark.rss.enabled
    assert(appConf("spark.rss.ha.master.hosts") == "0.0.0.0")
    assert(appConf("spark.rss.master.port") == "9097")
    assert(appConf("spark.shuffle.manager") == "org.apache.spark.shuffle.rss.RssShuffleManager")
    assert(appConf("spark.serializer") == "org.apache.spark.serializer.KryoSerializer")
    assert(appConf("spark.shuffle.service.enabled") == "false")
    assert(appConf("spark.dynamicAllocation.shuffleTracking.enabled") == "true")
    assert(appConf("spark.rss.limit.inflight.timeout") == "3600s")
    assert(appConf("spark.rss.shuffle.writer.mode") == "sort")
    assert(appConf("spark.rss.push.data.maxReqsInFlight") == "100000")
    assert(appConf("spark.rss.partition.split.threshold") == "1024M")

    // spark conf mapping should work
    assert(appConf("spark.pyspark.driver.python") == "./bin/python")

    assert(appConf(SPARK_AUX_JAR).contains("/path/hudi.jar"))

    // should add hbase datasource in hive
    assert(appConf("spark.sql.catalog.hbase") ==
      "org.apache.spark.sql.execution.datasources.v2.hbase.HBaseCatalog")
    assert(appConf("spark.sql.catalog.hbase.hive.metastore.uris") ==
      "thrift://hive.metastore")
    assert(appConf("spark.sql.catalog.hbase.hive.metastore.warehouse.dir") ==
      "/user/hive/warehouse")
    assert(appConf("spark.sql.catalog.hbase.spark.sql.warehouse.dir") ==
      "/user/hive/warehouse")
    assert(appConf(SPARK_AUX_JAR).contains(HBASE_JARS))
    assert(appConf(SPARK_SQL_DATASOURCE_CATALOG_IMPL) == "hive")

    // should merge spark-defaults.conf
    assert(appConf("spark.driver.extraClassPath") == "/default:/livy:/user")
    assert(!appConf.contains("spark.driver.extraLibraryPath"))
  }

}
