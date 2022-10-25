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

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.xml.XML

import okhttp3.{Headers, OkHttpClient, Request}
import org.apache.commons.lang3.StringUtils

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, Logging}
import org.apache.livy.client.common.ClientConf

object RssEnvProcessor {
  val RSC_CONF_PREFIX = "livy.rsc.yarn.cluster."

  val SPARK_MAJOR_VERSION = "spark.livy.spark_major_version"
  val SPARK_RSS_MIN_VERSION = 3

  @Deprecated
  val SPARK_RSS_ENABLED = "spark.rss.enabled"
  val SPARK_LIVY_RSS_ENABLED = "spark.livy.rss.enabled"
  val SPARK_YARN_QUEUE = "spark.yarn.queue"
  val YARN_CLUSTER_POLICY_LIST_URL = "policy.list.url"

  val SPARK_RSS_YARN_ALLOWED_MASTER_IDS = "livy.rsc.spark.rss.yarn.allowed.master.ids"
  val SPARK_RSS_YARN_PREDEFINED_QUEUES = "livy.rsc.spark.rss.yarn.predefined.queues"

  val defaultConf = Map(
    "spark.shuffle.manager" -> "org.apache.spark.shuffle.rss.RssShuffleManager",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.shuffle.service.enabled" -> "false",
    "spark.rss.push.data.maxReqsInFlight" -> "32"
  )
}

class RssEnvProcessor extends ApplicationEnvProcessor with Logging {

  import RssEnvProcessor._

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    val appConf = applicationEnvContext.appConf
    val yarnRouterMapping = YarnRouterMapping.apply(
      appConf.get(RSC_CONF_PREFIX + YARN_CLUSTER_POLICY_LIST_URL))
    val sparkMajorVersion = appConf.get(SPARK_MAJOR_VERSION).toInt

    var rssEnabled = Option(appConf.get(SPARK_LIVY_RSS_ENABLED))
      .orElse(Option(appConf.get(SPARK_RSS_ENABLED)))
    if (rssEnabled.isEmpty && sparkMajorVersion >= SPARK_RSS_MIN_VERSION) {
      val predefinedQueues = Option(appConf.get(SPARK_RSS_YARN_PREDEFINED_QUEUES))
      // Enable RSS if the user queue is included in predefined configuration.
      if (predefinedQueues.exists(_.split(",")
        .map(_.trim)
        .contains(appConf.get(SPARK_YARN_QUEUE)))) {
        logger.info(s"Enable RSS for queue ${appConf.get(SPARK_YARN_QUEUE)}")
        appConf.put(SPARK_LIVY_RSS_ENABLED, "true")
        rssEnabled = Option("true")
      }
    }

    rssEnabled.filter("true".equalsIgnoreCase)
      .foreach(_ => {
        if (sparkMajorVersion < SPARK_RSS_MIN_VERSION) {
          throw new ProcessorException(s"Unsupported Spark version $sparkMajorVersion for RSS.")
        }
        val masterYarn = appConf.get(ClientConf.LIVY_APPLICATION_MASTER_YARN_ID_KEY)
        if (masterYarn == null) {
          throw new ProcessorException(s"The master yarn must be a valid value for RSS.")
        }
        if (!appConf.get(SPARK_RSS_YARN_ALLOWED_MASTER_IDS).split(",")
          .map(_.toLowerCase().trim)
          .contains(masterYarn.toLowerCase())) {
          throw new ProcessorException(s"The master yarn $masterYarn is not allowed for RSS.")
        }
        val queue = appConf.get(SPARK_YARN_QUEUE)
        if (StringUtils.isBlank(queue)) {
          throw new ProcessorException("The queue must be set by user.")
        }
        val yarnCluster = yarnRouterMapping.getCluster(queue)
        appConf.asScala.filter { kv =>
          StringUtils.startsWith(kv._1, RSC_CONF_PREFIX + yarnCluster)
        }.foreach { kv =>
          appConf.put(StringUtils.substringAfter(kv._1, RSC_CONF_PREFIX + yarnCluster + "."), kv._2)
        }
        appConf.putAll(defaultConf.asJava)
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

class YarnRouterMapping(policyListUrl: String) extends Logging {

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

  private val httpClient: OkHttpClient = new OkHttpClient()

  private val headers: Map[String, String] =
    Map("Content-type" -> "application/xml", "Accept" -> "application/xml")

  def startLoadMapping(): Unit = {
    info(s"Start load yarn cluster policy mapping.")
    executor.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = {
          val policyMap = loadXmlFromUrl(policyListUrl)
          updatePolicyListCache(policyMap)
        }
      }, 0, refreshInterval, TimeUnit.MINUTES)
  }

  def getCluster(queue: String): String = {
    policyListCache.asScala.getOrElse(queue, {
      val policyMap = loadXmlFromUrl(policyListUrl)
      updatePolicyListCache(policyMap)
      val cluster = policyMap.get(queue)
      if (cluster == null) {
        throw new ProcessorException(s"Not found yarn cluster for queue $queue.")
      }
      cluster
    })
  }

  private def loadXmlFromUrl(url: String): java.util.Map[String, String] = {
    info(s"loading yarn cluster policy mapping.")
    try {
      val requestBuilder = new Request.Builder()
      requestBuilder.headers(Headers.of(headers.asJava))
      val res = httpClient.newCall(requestBuilder.url(url).build()).execute()
      val body = res.body().string()
      res.body().close()

      val policyMap = new java.util.HashMap[String, String]()
      (XML.loadString(body) \\ "policies").foreach(policy => {
        val k = policy \\ "queueName"
        val v = policy \\ "cluster"
        val weight = policy \\ "weight"
        if (weight.length == 1 && weight.text.toDouble == 100.0) {
          policyMap.put(k.text, v.text)
        }
      })
      policyMap
    } catch {
      case e: Exception =>
        error(s"failed to load yarn cluster policy mapping.", e)
        throw new ProcessorException(e.getMessage, e.getCause)
    }
  }

  // Visible for testing
  private[livy] def updatePolicyListCache(updatedMap: java.util.Map[String, String]): Unit = {
    info(s"updating yarn cluster policy mapping cache.")
    policyListCache.asScala.foreach(kv => {
      if (!updatedMap.containsKey(kv._1)) {
        info(s"Deleting invalid policy mapping cache key ${kv._1}.")
        policyListCache.remove(kv._1)
      }
    })
    policyListCache.putAll(updatedMap)
  }
}
