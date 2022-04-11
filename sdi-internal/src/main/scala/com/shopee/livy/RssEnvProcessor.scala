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

import java.net.URL
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.xml.XML

import org.apache.commons.lang3.StringUtils

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, Logging}

object RssEnvProcessor {
  val RSC_CONF_PREFIX = "livy.rsc.yarn.cluster."

  val SPARK_RSS_ENABLED = "spark.rss.enabled"
  val SPARK_YARN_QUEUE = "spark.yarn.queue"
  val YARN_CLUSTER_POLICY_LIST_URL = "policy.list.url"
}

class RssEnvProcessor extends ApplicationEnvProcessor with Logging {

  import RssEnvProcessor._

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    val appConf = applicationEnvContext.appConf
    val yarnRouterMapping = YarnRouterMapping.apply(
      appConf.get(RSC_CONF_PREFIX + YARN_CLUSTER_POLICY_LIST_URL))
    val rssEnabled = appConf.get(SPARK_RSS_ENABLED)

    Option(rssEnabled).filter("true".equalsIgnoreCase).foreach(_ => {
      val queue = appConf.get(SPARK_YARN_QUEUE)
      if (StringUtils.isBlank(queue)) {
        throw new Exception("The queue must be set by user.")
      }
      val yarnCluster = yarnRouterMapping.getCluster(queue)
      appConf.asScala.filter { kv =>
        StringUtils.startsWith(kv._1, RSC_CONF_PREFIX + yarnCluster)
      }.foreach { kv =>
        appConf.put(StringUtils.substringAfter(kv._1, RSC_CONF_PREFIX + yarnCluster + "."), kv._2)
      }
      appConf.put("spark.shuffle.manager", "org.apache.spark.shuffle.rss.RssShuffleManager")
      appConf.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      appConf.put("spark.shuffle.service.enabled", "false")
      appConf.put("spark.dynamicAllocation.shuffleTracking.enabled", "true")
    })
  }
}

object YarnRouterMapping {

  private val refreshInterval = 3

  // Mock for testing
  private[livy] var mockYarnRouterMapping: YarnRouterMapping = _

  private val YARNROUTERMAPPING_CONSTRUCTOR_LOCK = new Object()

  private val yarnRouterMapping: AtomicReference[YarnRouterMapping] =
    new AtomicReference[YarnRouterMapping](null)

  def apply(policyListUrl: String): YarnRouterMapping = {
    if (mockYarnRouterMapping != null) {
      return mockYarnRouterMapping
    }
    YARNROUTERMAPPING_CONSTRUCTOR_LOCK.synchronized {
      if (yarnRouterMapping.get() == null) {
        yarnRouterMapping.set(new YarnRouterMapping(policyListUrl))
        yarnRouterMapping.get().startLoadMapping()
      }
      yarnRouterMapping.get()
    }
  }
}

class YarnRouterMapping(policyListUrl: String) {

  import YarnRouterMapping._

  // Visible for testing
  private[livy] val policyListCache = new ConcurrentHashMap[String, String]()

  private val executor: ScheduledExecutorService = Executors.newScheduledThreadPool(1,
    new ThreadFactory() {
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r)
        thread.setName("yarn-router-policy-refresh-thread")
        thread.setDaemon(true)
        thread
      }
    })

  def startLoadMapping(): Unit = {
    executor.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = {
          val policyMap = loadXmlFromUrl(new URL(policyListUrl))
          updatePolicyListCache(policyMap)
        }
      }, 0, refreshInterval, TimeUnit.MINUTES)
  }

  def getCluster(queue: String): String = {
    policyListCache.asScala.getOrElse(queue, {
      val policyMap = loadXmlFromUrl(new URL(policyListUrl))
      updatePolicyListCache(policyMap)
      val cluster = policyMap.get(queue)
      if (cluster == null) {
        throw new Exception(s"Not found yarn cluster for queue $queue.")
      }
      cluster
    })
  }

  private def loadXmlFromUrl(url: URL): java.util.Map[String, String] = {
    val policyMap = new java.util.HashMap[String, String]()
    (XML.load(url) \\ "policies").foreach(policy => {
      val k = policy \\ "queueName"
      val v = policy \\ "cluster"
      val weight = policy \\ "weight"
      if (weight.length == 1 && weight.text.toDouble == 100.0) {
        policyMap.put(k.text, v.text)
      }
    })
    policyMap
  }

  // Visible for testing
  private[livy] def updatePolicyListCache(updatedMap: java.util.Map[String, String]): Unit = {
    policyListCache.asScala.foreach(kv => {
      if (!updatedMap.containsKey(kv._1)) {
        policyListCache.remove(kv._1)
      }
    })
    policyListCache.putAll(updatedMap)
  }
}
