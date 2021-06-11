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

package org.apache.livy.sessions

import java.lang.reflect.{Constructor, InvocationTargetException}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.util.control.Breaks.{break, breakable}

import org.apache.zookeeper.KeeperException.BadVersionException
import org.apache.zookeeper.data.Stat

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.cluster.ClusterManager
import org.apache.livy.server.recovery.{SessionManagerState, SessionStore, ZooKeeperManager}

object SessionIdGenerator extends Logging {
  def apply(livyConf: LivyConf, sessionStore: SessionStore, clusterManager: Option[ClusterManager],
      zooKeeperManager: Option[ZooKeeperManager]): SessionIdGenerator = {
    try {
      val clz: Class[_] = Class.forName(livyConf.get(LivyConf.SESSION_ID_GENERATOR_CLASS))
      val constructor: Constructor[_] = clz.getConstructor(classOf[LivyConf], classOf[SessionStore],
        classOf[Option[ClusterManager]], classOf[Option[ZooKeeperManager]])
      constructor.newInstance(livyConf, sessionStore, clusterManager, zooKeeperManager)
        .asInstanceOf[SessionIdGenerator]
    } catch {
      case e@(_: NoSuchMethodException | _: InstantiationException
              | _: IllegalAccessException | _: InvocationTargetException
              | _: ClassNotFoundException) =>
        error("Unable to instantiate session id generator class "
          + livyConf.get(LivyConf.SESSION_ID_GENERATOR_CLASS), e)
        throw new IllegalArgumentException(e)
    }
  }
}

abstract class SessionIdGenerator(
    livyConf: LivyConf,
    sessionStore: SessionStore,
    clusterManager: Option[ClusterManager],
    zooKeeperManager: Option[ZooKeeperManager]) {

  def nextId(sessionType: String): Int

  def isGlobalUnique(): Boolean
}

class InMemorySessionIdGenerator(
    livyConf: LivyConf,
    sessionStore: SessionStore,
    clusterManager: Option[ClusterManager],
    zooKeeperManager: Option[ZooKeeperManager])
  extends SessionIdGenerator(livyConf, sessionStore,
    clusterManager, zooKeeperManager) {

  private val idCounterMap = new ConcurrentHashMap[String, AtomicInteger]()

  private def idCounter(sessionType: String): AtomicInteger = {
    if (!idCounterMap.contains(sessionType)) {
      val idCounter = new AtomicInteger(0)
      idCounter.set(sessionStore.getNextSessionId(sessionType))
      idCounterMap.putIfAbsent(sessionType, idCounter)
    }
    idCounterMap.get(sessionType)
  }

  override def nextId(sessionType: String): Int = synchronized {
    val result = idCounter(sessionType).getAndIncrement()
    sessionStore.saveNextSessionId(sessionType, result + 1)
    result
  }

  override  def isGlobalUnique(): Boolean = {
    false
  }
}

class ZookeeperSessionIdGenerator(
    livyConf: LivyConf,
    sessionStore: SessionStore,
    clusterManager: Option[ClusterManager],
    optionZooKeeperManager: Option[ZooKeeperManager])
  extends SessionIdGenerator(livyConf, sessionStore,
    clusterManager, optionZooKeeperManager)
  with Logging {

  require(optionZooKeeperManager.isDefined, "ZookeeperSessionIdGenerator requires zooKeeperManager")
  val zooKeeperManager = optionZooKeeperManager.get

  private def sessionManagerPath(sessionType: String): String = {
    Option(livyConf.get(LivyConf.SESSION_ID_GENERATOR_ZK_KEY_PREFIX))
      .map(c => s"${c}/$sessionType")
      .getOrElse(sessionStore.sessionManagerPath(sessionType))
  }

  override def nextId(sessionType: String): Int = synchronized {
    var result = -1
    breakable({
      for (i <- 1 to Integer.MAX_VALUE) {
        try {
          val stat = new Stat()
          val path = sessionManagerPath(sessionType)
          val cManagerState = zooKeeperManager.get[SessionManagerState](path, Option(stat))
            .getOrElse(SessionManagerState(0))
          result = cManagerState.nextSessionId
          val nManagerState = SessionManagerState(cManagerState.nextSessionId + 1)
          zooKeeperManager.set(path, nManagerState, Option(stat.getVersion))
          break()
        } catch {
          case badVersionException: BadVersionException =>
            warn(s"Session id CAS failed ${i} times")
        }
      }
    })
    result
  }

  override  def isGlobalUnique(): Boolean = {
    true
  }
}
