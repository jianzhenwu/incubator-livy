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

import scala.collection.mutable
import scala.collection.JavaConverters._

import org.scalatest.{BeforeAndAfterAll, FunSuite}

class RssEnvProcessorSpec extends FunSuite with BeforeAndAfterAll {

}

class YarnRouterMappingSpec extends RssEnvProcessorSpec {

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
