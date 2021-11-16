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

package org.apache.livy.cluster

import java.lang.reflect.{Constructor, InvocationTargetException}
import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.ClassTag
import scala.util.{Random, Try}
import scala.util.hashing.MurmurHash3

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex

import org.apache.livy.{LivyConf, Logging, ServerMetadata}
import org.apache.livy.server.recovery.{SessionStore, ZooKeeperManager}
import org.apache.livy.sessions.Session.RecoveryMetadata

object SessionAllocator extends Logging {
  def apply(
      livyConf: LivyConf,
      clusterManager: ClusterManager,
      sessionStore: SessionStore,
      zkManager: ZooKeeperManager,
      serverAllocator: ServerAllocator): SessionAllocator = {
    try {
      val clz: Class[_] = Class.forName(livyConf.get(LivyConf.CLUSTER_SESSION_ALLOCATOR_CLASS))
      val constructor: Constructor[_] = clz.getConstructor(classOf[LivyConf],
        classOf[ClusterManager], classOf[SessionStore], classOf[ZooKeeperManager],
        classOf[ServerAllocator])
      val sessionAllocator = constructor.newInstance(livyConf, clusterManager, sessionStore,
        zkManager, serverAllocator).asInstanceOf[SessionAllocator]
      clusterManager.registerNodeJoinListener(sessionAllocator.onServerJoin)
      clusterManager.registerNodeLeaveListener(sessionAllocator.onServerLeave)
      sessionAllocator
    } catch {
      case e@(_: NoSuchMethodException | _: InstantiationException
              | _: IllegalAccessException | _: InvocationTargetException
              | _: ClassNotFoundException) =>
        error("Unable to instantiate session allocator class "
          + livyConf.get(LivyConf.CLUSTER_SESSION_ALLOCATOR_CLASS), e)
        throw new IllegalArgumentException(e)
    }
  }
}

abstract class SessionAllocator(
    livyConf: LivyConf,
    clusterManager: ClusterManager,
    sessionStore: SessionStore,
    zkManager: ZooKeeperManager,
    serverAllocator: ServerAllocator) {

  /**
   * Return ServerNode of the session, no matter if server is online;
   * return None if session not found
   * @param sessionType
   * @param sessionId
   * @return
   */
  def findServer[T <: RecoveryMetadata](
      sessionType: String,
      sessionId: Int)(implicit t: ClassTag[T]): Option[ServerNode]

  /**
   * Allocate server for the session and return the ServerNode.
   * How offline server handled depends on allocation algo
   * @param sessionType
   * @param sessionId
   * @return
   */
  def allocateServer[T <: RecoveryMetadata](
      sessionType: String,
      sessionId: Int,
      owner: String)(implicit t: ClassTag[T]): ServerNode

  /**
   * Deallocate session from a server if session is deallocatable.
   * Throw IllegalStateException if not.
   * @param sessionType
   * @param sessionId
   */
  def deallocateServer[T <: RecoveryMetadata](
      sessionType: String,
      sessionId: Int)(implicit t: ClassTag[T]): Unit

  def onServerJoin(serverNode: ServerNode): Unit

  def onServerLeave(serverNode: ServerNode): Unit

  def getAllSessions[T <: RecoveryMetadata : ClassTag](
        sessionType: String,
        serverMetadata: Option[ServerMetadata] = None): Seq[Try[T]]
}

class StateStoreMappingSessionAllocator(
    livyConf: LivyConf,
    clusterManager: ClusterManager,
    sessionStore: SessionStore,
    zkManager: ZooKeeperManager,
    serverAllocator: ServerAllocator)
  extends SessionAllocator(livyConf, clusterManager, sessionStore, zkManager,
    serverAllocator) with Logging {

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class StateStoreMappingRecoveryMetadata(
      id: Int,
      serverMetadata: ServerMetadata,
      version: Int = 1) extends RecoveryMetadata {
    def this(id: Int) = this(id, ServerMetadata("", -1))
    @JsonIgnore
    def isServerDeallocatable(): Boolean = { true }
    @JsonIgnore
    override def isRecoverable(): Boolean = { false }
  }

  private val lockCount = livyConf.getInt(LivyConf.CLUSTER_SESSION_ALLOCATOR_STATE_STORE_LOCK_COUNT)
  private val lockZkPath: String = {
    val configLockPath = livyConf.get(LivyConf.CLUSTER_SESSION_ALLOCATOR_STATE_STORE_LOCK_ZK_PATH)
    if (configLockPath.startsWith("/")) {
      configLockPath
    } else {
      s"/$configLockPath"
    }
  }
  private val locks = {
    val locks = scala.collection.mutable.Map[Int, InterProcessSemaphoreMutex]()
    for (i <- 0 until lockCount) {
      locks(i) = zkManager.createLock(s"$lockZkPath/$i")
    }
    locks.toMap
  }

  private def lockOf(sessionId: Int): InterProcessSemaphoreMutex = {
    locks(sessionId % lockCount)
  }

  private val algo = livyConf.get(LivyConf.CLUSTER_SESSION_ALLOCATOR_STATE_STORE_ALGO)
  private val roundRobinCount = new AtomicInteger(-1)

  override def findServer[T <: RecoveryMetadata](
      sessionType: String,
      sessionId: Int)(implicit t: ClassTag[T]): Option[ServerNode] = {

    sessionStore.get[T](sessionType, sessionId).map(recoveryMetadata => {
      require(Option(recoveryMetadata.serverMetadata.host).forall(_.isEmpty) != true,
        s"Server host inside state store of $sessionType with id $sessionId is null")
      require(recoveryMetadata.serverMetadata.port > 0,
        s"Server port inside state store of $sessionType with id $sessionId not valid")

      ServerNode(recoveryMetadata.serverMetadata)
    })
  }

  override def allocateServer[T <: RecoveryMetadata](
      sessionType: String,
      sessionId: Int,
      owner: String = null)(implicit t: ClassTag[T]): ServerNode = {

    val serverNodes = serverAllocator.allocateServer[T](sessionType, sessionId, owner)

    val index = algo match {
      case "RANDOM" =>
        new Random().nextInt(serverNodes.size)
      case "HASH" =>
        MurmurHash3.stringHash(sessionId.toString).abs % serverNodes.size
      case "ROUND-ROBIN" =>
        roundRobinCount.incrementAndGet() % serverNodes.size
    }

    val serverNode: ServerNode = serverNodes.toArray[ServerNode]
      .sortWith(_.timestamp < _.timestamp).apply(index)

    val lock = lockOf(sessionId)
    try {
      lock.acquire()

      val recoveryMetadata: RecoveryMetadata = sessionStore.get[T](sessionType, sessionId)
        .getOrElse({
          new StateStoreMappingRecoveryMetadata(sessionId)
        })
      val recoveryServerMetadata = recoveryMetadata.serverMetadata
      if (recoveryServerMetadata.isValid
          && clusterManager.isNodeOnline(ServerNode(recoveryServerMetadata))) {
        warn(s"$sessionType session $sessionId is on online server: $recoveryServerMetadata, " +
          s"probably relocate by another request or offline server is back")
        ServerNode(recoveryServerMetadata)
      } else {
        recoveryServerMetadata.host = serverNode.serverMetadata.host
        recoveryServerMetadata.port = serverNode.serverMetadata.port
        sessionStore.save(sessionType, recoveryMetadata)
        serverNode
      }
    } finally {
      lock.release()
    }
  }

  override def deallocateServer[T <: RecoveryMetadata](
      sessionType: String,
      sessionId: Int)(implicit t: ClassTag[T]): Unit = {
    sessionStore.get[T](sessionType, sessionId).map(recoveryMetadata => {
      if (recoveryMetadata.isServerDeallocatable()) {
        sessionStore.remove(sessionType, sessionId)
      } else {
        throw new IllegalStateException(s"Not able to deallocate $sessionType session $sessionId")
      }
    })
  }

  override def onServerJoin(serverNode: ServerNode): Unit = {
    // Do nothing
  }

  override def onServerLeave(serverNode: ServerNode): Unit = {
    // If this server leaves cluster, sessions of this server may be taken by other server,
    // but SessionManager of this server still has sessions, we need to clear them.
    // Remove session from SessionManage need to clear state of SparkApp which involve many changes,
    // so just shutdown this server to simplify logic
    if (serverNode.serverMetadata == livyConf.serverMetadata()) {
      error("Server leaves cluster, exit to reset SessionManager")
      System.exit(-1)
    }
  }

  override def getAllSessions[T <: RecoveryMetadata : ClassTag](
      sessionType: String,
      serverMetadata: Option[ServerMetadata]): Seq[Try[T]] = {
    sessionStore.getAllSessions(sessionType, serverMetadata)
  }
}

