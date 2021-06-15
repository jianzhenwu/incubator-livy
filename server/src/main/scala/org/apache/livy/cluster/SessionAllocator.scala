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
import scala.util.Random
import scala.util.hashing.MurmurHash3

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

import org.apache.livy.{LivyConf, Logging, ServerMetadata}
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.Session.RecoveryMetadata

object SessionAllocator extends Logging {
  def apply(
      livyConf: LivyConf,
      clusterManager: ClusterManager,
      sessionStore: SessionStore): SessionAllocator = {
    try {
      val clz: Class[_] = Class.forName(livyConf.get(LivyConf.CLUSTER_SESSION_ALLOCATOR_CLASS))
      val constructor: Constructor[_] = clz.getConstructor(classOf[LivyConf],
        classOf[ClusterManager], classOf[SessionStore])
      constructor.newInstance(livyConf, clusterManager, sessionStore)
        .asInstanceOf[SessionAllocator]
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
    sessionStore: SessionStore) {
  def findServer[T <: RecoveryMetadata](
      sessionType: String,
      sessionId: Int)(implicit t: ClassTag[T]): Option[ServerNode]

  def allocateServer[T <: RecoveryMetadata](
      sessionType: String,
      sessionId: Int)(implicit t: ClassTag[T]): ServerNode

  def onServerJoin(serverNode: ServerNode): Unit

  def onServerLeave(serverNode: ServerNode): Unit
}

class StateStoreMappingSessionAllocator(
    livyConf: LivyConf,
    clusterManager: ClusterManager,
    sessionStore: SessionStore) extends SessionAllocator(livyConf, clusterManager, sessionStore) {

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class StateStoreMappingRecoveryMetadata(
      id: Int,
      serverMetadata: ServerMetadata,
      version: Int = 1) extends RecoveryMetadata {
    def this(id: Int) = this(id, ServerMetadata("", -1))
  }

  val algo = livyConf.get(LivyConf.CLUSTER_SESSION_ALLOCATOR_STATE_STORE_ALGO)
  val roundRobinCount = new AtomicInteger(-1)

  override def findServer[T <: RecoveryMetadata](
      sessionType: String,
      sessionId: Int)(implicit t: ClassTag[T]): Option[ServerNode] = {

    val serverNode = sessionStore.get[T](sessionType, sessionId).map(recoveryMetadata => {
      require(Option(recoveryMetadata.serverMetadata.host).forall(_.isEmpty) != true,
        s"Server host inside state store of $sessionType with id $sessionId is null")
      require(recoveryMetadata.serverMetadata.port > 0,
        s"Server port inside state store of $sessionType with id $sessionId not valid")

      clusterManager.getNodes().find(server => {
        server.serverMetadata == recoveryMetadata.serverMetadata
      }).orNull
    })

    if (serverNode.isEmpty || serverNode.get == null) {
      None
    } else {
      serverNode
    }
  }

  override def allocateServer[T <: RecoveryMetadata](
      sessionType: String,
      sessionId: Int)(implicit t: ClassTag[T]): ServerNode = {
    val serverNodes = clusterManager.getNodes()
    val index = algo match {
      case "RANDOM" =>
        new Random().nextInt(serverNodes.size)
      case "HASH" =>
        MurmurHash3.stringHash(sessionId.toString).abs % serverNodes.size
      case "ROUND-ROBIN" =>
        roundRobinCount.incrementAndGet() % serverNodes.size
    }

    val serverNode: ServerNode = serverNodes.toArray[ServerNode].apply(index)

    val recoveryMetadata: RecoveryMetadata = sessionStore.get[T](sessionType, sessionId).getOrElse({
      new StateStoreMappingRecoveryMetadata(sessionId)
    })
    recoveryMetadata.serverMetadata.host = serverNode.serverMetadata.host
    recoveryMetadata.serverMetadata.port = serverNode.serverMetadata.port
    sessionStore.save(sessionType, recoveryMetadata)

    serverNode
  }

  override def onServerJoin(serverNode: ServerNode): Unit = {
    // Do nothing
  }

  override def onServerLeave(serverNode: ServerNode): Unit = {
    // Do nothing
  }
}

