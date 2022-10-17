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

import scala.collection.mutable
import scala.collection.JavaConverters._

import org.mockito.Matchers.anyString
import org.mockito.Mockito.{mock, when}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.livy.ApplicationEnvContext

class RssEnvProcessorSpec extends FunSuite with BeforeAndAfterAll {

  private val appConf = new mutable.HashMap[String, String]

  override def beforeAll(): Unit = {
    appConf += (
      RssEnvProcessor.SPARK_LIVY_RSS_ENABLED -> "false",
      RssEnvProcessor.SPARK_YARN_QUEUE -> "queue1",
      RssEnvProcessor.RSC_CONF_PREFIX + RssEnvProcessor.YARN_CLUSTER_POLICY_LIST_URL ->
        "http://0.0.0.0/url",
      "livy.rsc.yarn.cluster.cluster1.spark.rss.ha.master.hosts" -> "0.0.0.0",
      "livy.rsc.yarn.cluster.cluster1.spark.rss.master.port" -> "9097",
      "livy.rsc.spark.rss.yarn.allowed.master.ids" -> "default, backup",
      "livy.application.master-yarn-id" -> "default"
    )
  }

  test("should not enable RSS when without predefined configuration") {
    val yarnRouterMapping = mock(classOf[YarnRouterMapping])
    YarnRouterMapping.mockYarnRouterMapping = yarnRouterMapping
    when(yarnRouterMapping.getCluster(anyString())).thenReturn("cluster1")

    val context = ApplicationEnvContext(new util.HashMap[String, String](), appConf.asJava)
    val processor = new RssEnvProcessor()
    processor.process(context)

    assert(appConf("spark.livy.rss.enabled") == "false")
  }

  test("should not enable RSS when using non-predefined queue") {
    val yarnRouterMapping = mock(classOf[YarnRouterMapping])
    YarnRouterMapping.mockYarnRouterMapping = yarnRouterMapping
    when(yarnRouterMapping.getCluster(anyString())).thenReturn("cluster1")

    appConf += "livy.rsc.spark.rss.yarn.predefined.queues" -> "queue2, queue3"

    val context = ApplicationEnvContext(new util.HashMap[String, String](), appConf.asJava)
    val processor = new RssEnvProcessor()
    processor.process(context)

    assert(appConf("spark.livy.rss.enabled") == "false")
  }

  test("should enable RSS if the user queue is included in predefined configuration") {
    val yarnRouterMapping = mock(classOf[YarnRouterMapping])
    YarnRouterMapping.mockYarnRouterMapping = yarnRouterMapping
    when(yarnRouterMapping.getCluster(anyString())).thenReturn("cluster1")

    appConf += "livy.rsc.spark.rss.yarn.predefined.queues" -> "queue1, queue2"

    val context = ApplicationEnvContext(new util.HashMap[String, String](), appConf.asJava)
    val processor = new RssEnvProcessor()
    processor.process(context)

    assert(appConf("spark.livy.rss.enabled") == "true")
    assert(appConf("spark.rss.ha.master.hosts") == "0.0.0.0")
    assert(appConf("spark.rss.master.port") == "9097")
    assert(appConf("spark.shuffle.manager") == "org.apache.spark.shuffle.rss.RssShuffleManager")
    assert(appConf("spark.serializer") == "org.apache.spark.serializer.KryoSerializer")
    assert(appConf("spark.shuffle.service.enabled") == "false")
    assert(appConf("spark.rss.push.data.maxReqsInFlight") == "32")
  }
}

class YarnRouterMappingSpec extends FunSuite with BeforeAndAfterAll {

  private val policyListUrl = "http://0.0.0.0/url"
  private var yarnRouterMapping: YarnRouterMapping = _

  override def beforeAll(): Unit = {
    yarnRouterMapping = new YarnRouterMapping(policyListUrl)
  }

  test("update policy mapping cache") {
    val updatedMap = mutable.HashMap[String, String](
      "queue1" -> "cluster1", "queue2" -> "cluster2", "queue3" -> "cluster3")

    yarnRouterMapping.updatePolicyListCache(updatedMap.asJava)
    assert(yarnRouterMapping.policyListCache.size() == 3)
    assert(yarnRouterMapping.policyListCache.containsKey("queue1"))
    assert(yarnRouterMapping.policyListCache.containsKey("queue2"))
    assert(yarnRouterMapping.policyListCache.containsKey("queue3"))

    updatedMap.remove("queue1")
    yarnRouterMapping.updatePolicyListCache(updatedMap.asJava)
    assert(yarnRouterMapping.policyListCache.size() == 2)
    assert(!yarnRouterMapping.policyListCache.containsKey("queue1"))

    updatedMap.put("queue2", "cluster0")
    yarnRouterMapping.updatePolicyListCache(updatedMap.asJava)
    assert(yarnRouterMapping.policyListCache.size() == 2)
    assert(yarnRouterMapping.policyListCache.get("queue2") == "cluster0")
  }
}
