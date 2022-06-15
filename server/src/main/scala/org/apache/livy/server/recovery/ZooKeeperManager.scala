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

package org.apache.livy.server.recovery

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.api.{BackgroundCallback, CuratorEvent, CuratorEventType, UnhandledErrorListener}
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.curator.retry.RetryNTimes
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.NoNodeException
import org.apache.zookeeper.data.Stat

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.metrics.common.{Metrics, MetricsKey}
import org.apache.livy.utils.LivyUncaughtException

class ZooKeeperManager(
    livyConf: LivyConf,
    mockCuratorClient: Option[CuratorFramework] = None)
  extends JsonMapper with Logging {

  def this(livyConf: LivyConf) {
    this(livyConf, None)
  }

  private val retryValue = Option(livyConf.get(LivyConf.ZK_RETRY_POLICY)).
    orElse(Option(livyConf.get(LivyConf.RECOVERY_ZK_STATE_STORE_RETRY_POLICY))).
    map(_.trim).orNull

  require(retryValue != null && !retryValue.isEmpty,
    s"Please config ${LivyConf.ZK_RETRY_POLICY.key}.")

  // a regex to match patterns like "m, n" where m and n both are integer values
  private val retryPattern = """\s*(\d+)\s*,\s*(\d+)\s*""".r
  private[recovery] val retryPolicy = retryValue match {
    case retryPattern(n, sleepMs) => new RetryNTimes(n.toInt, sleepMs.toInt)
    case _ => throw new IllegalArgumentException(
      s"contains bad value: $retryValue. " +
        "Correct format is <max retry count>,<sleep ms between retry>. e.g. 5,100")
  }

  private val curatorClient = mockCuratorClient.getOrElse {
    val zkAddress = Option(livyConf.get(LivyConf.ZOOKEEPER_URL)).
      orElse(Option(livyConf.get(LivyConf.RECOVERY_STATE_STORE_URL))).
      map(_.trim).orNull

    require(zkAddress != null && !zkAddress.isEmpty,
      s"Please config ${LivyConf.ZOOKEEPER_URL.key}.")

    CuratorFrameworkFactory.newClient(zkAddress, retryPolicy)
  }

  curatorClient.getUnhandledErrorListenable().addListener(new UnhandledErrorListener {
    def unhandledError(message: String, e: Throwable): Unit = {
      error(s"Fatal Zookeeper error: ${message}.", e)
      throw new LivyUncaughtException(e.getMessage)
    }
  })

  def start(): Unit = {
    curatorClient.start()
  }

  def stop(): Unit = {
    curatorClient.close()
  }

  // TODO Make sure ZK path has proper secure permissions so that other users cannot read its
  // contents.
  def set(key: String, value: Object, version: Option[Int] = None): Unit = {
    val data = serializeToBytes(value)
    if (curatorClient.checkExists().forPath(key) == null) {
      curatorClient.create().creatingParentsIfNeeded().forPath(key, data)
    } else {
      if (version.isEmpty) {
        curatorClient.setData().forPath(key, data)
      } else {
        curatorClient.setData().withVersion(version.get).forPath(key, data)
      }
    }
  }

  def awaitSync(key: String): Boolean = {
    val latch = new CountDownLatch(1)
    Metrics().incrementCounter(MetricsKey.ZK_MANAGER_SYNC_TOTAL_COUNT)
    val callback = new BackgroundCallback() {
      override def processResult(client: CuratorFramework, event: CuratorEvent): Unit = {
        if (event.getType eq CuratorEventType.SYNC) latch.countDown()
      }
    }
    curatorClient.sync.inBackground(callback).forPath(key)
    val timeout = livyConf.getTimeAsMs(LivyConf.ZK_SYNC_TIMEOUT)
    var isSyncSuccess = false
    try {
      Metrics().startStoredScope(MetricsKey.ZK_MANAGER_SYNC_PROCESSING_TIME)
      isSyncSuccess = latch.await(timeout, TimeUnit.MILLISECONDS)
      if (!isSyncSuccess) {
        Metrics().incrementCounter(MetricsKey.ZK_MANAGER_SYNC_TIMEOUT_COUNT)
      }
    } finally {
      Metrics().endStoredScope(MetricsKey.ZK_MANAGER_SYNC_PROCESSING_TIME)
    }
    isSyncSuccess
  }

  def get[T: ClassTag](key: String, stat: Option[Stat] = None): Option[T] = {
    if (awaitSync(key)) {
      if (curatorClient.checkExists().forPath(key) == null) {
        None
      } else {
        if (stat.isEmpty) {
          Option(deserialize[T](curatorClient.getData().forPath(key)))
        } else {
          Option(deserialize[T](curatorClient.getData().storingStatIn(stat.get).forPath(key)))
        }
      }
    } else {
      throw new Exception(s"curatorClient sync timeout.")
    }
  }

  def getChildren(key: String): Seq[String] = {
    if (awaitSync(key)) {
      if (curatorClient.checkExists().forPath(key) == null) {
        Seq.empty[String]
      } else {
        curatorClient.getChildren.forPath(key).asScala
      }
    } else {
      throw new Exception(s"curatorClient sync timeout.")
    }
  }

  def remove(key: String): Unit = {
    try {
      curatorClient.delete().guaranteed().forPath(key)
    } catch {
      case _: NoNodeException => warn(s"Fail to remove non-existed zookeeper node: ${key}")
    }
  }

  def createEphemeralNode(path: String, value: Object): Unit = {
    val data = serializeToBytes(value)
    curatorClient.create.creatingParentsIfNeeded.withMode(CreateMode.EPHEMERAL).forPath(path, data)
  }

  // For test
  protected def getPathChildrenCache(path: String): PathChildrenCache = {
    new PathChildrenCache(curatorClient, path, true)
  }

  def watchChildrenNodes[T: ClassTag](
      path: String,
      nodeEventHandler: (Type, Option[String], Option[T]) => Unit): Unit = {
    val cache = getPathChildrenCache(path)
    cache.start(StartMode.BUILD_INITIAL_CACHE)

    val listener = new PathChildrenCacheListener() {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
        val eData = event.getData
        val eType = event.getType
        if (List(Type.CHILD_ADDED, Type.CHILD_UPDATED, Type.CHILD_REMOVED).contains(eType)) {
          nodeEventHandler(eType, Some(eData.getPath), Some(deserialize[T](eData.getData)))
        } else {
          nodeEventHandler(eType, None, None)
        }
      }
    }

    cache.getListenable.addListener(listener)
  }

  def createLock(lockDir: String): InterProcessSemaphoreMutex = {
    new InterProcessSemaphoreMutex(curatorClient, lockDir)
  }
}
