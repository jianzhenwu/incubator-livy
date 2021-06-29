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

import scala.collection.immutable.Set
import scala.collection.mutable.{ArrayBuffer, HashSet}

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.LivyConf.CLUSTER_ZK_SERVER_REGISTER_KEY_PREFIX
import org.apache.livy.server.recovery.ZooKeeperManager

class ZookeeperClusterManager(livyConf: LivyConf, zkManager: ZooKeeperManager,
    mockServerNode: Option[ServerNode] = None)
  extends ClusterManager with Logging {
  private val serverMetadata = livyConf.serverMetadata()
  private val serverRegisterKeyPrefix: String = {
    val configKeyPrefix = livyConf.get(CLUSTER_ZK_SERVER_REGISTER_KEY_PREFIX)
    if (configKeyPrefix.startsWith("/")) {
      configKeyPrefix
    } else {
      s"/$configKeyPrefix"
    }
  }
  private val serverPath = serverRegisterKeyPrefix + "/" +
    serverMetadata.host + ":" + serverMetadata.port
  private val serverNode = mockServerNode
    .getOrElse(ServerNode(serverMetadata, System.currentTimeMillis()))

  private val nodes = new HashSet[ServerNode]()
  private val nodeJoinListeners = new ArrayBuffer[ServerNode => Unit]()
  private val nodeLeaveListeners = new ArrayBuffer[ServerNode => Unit]()

  // Start listening
  zkManager.watchChildrenNodes(serverRegisterKeyPrefix,
    (eType: Type, ePath: Option[String], eData: Option[ServerNode]) => {
      if (eType == Type.CHILD_ADDED) {
        nodeAddHandler(ePath.get, eData.get)
      } else if (eType == Type.CHILD_REMOVED) {
        nodeRemoveHandler(ePath.get, eData.get)
      } else if (eType == Type.CONNECTION_LOST) {
        nodeRemoveHandler(serverPath, serverNode)
      }
    })

  zkManager.getChildren(serverRegisterKeyPrefix).foreach(node => {
    val serviceNode = zkManager.get[ServerNode](serverRegisterKeyPrefix + "/" + node).get
    nodes.add(serviceNode)
  })

  override def register(): Unit = {
    zkManager.createEphemeralNode(serverPath, serverNode)
  }

  override def getNodes(): Set[ServerNode] = {
    nodes.toSet
  }

  override def isNodeOnline(serverNode: ServerNode): Boolean = {
    getNodes().find(_.serverMetadata == serverNode.serverMetadata).isDefined
  }

  override def registerNodeJoinListener(listener: ServerNode => Unit): Unit = {
    nodeJoinListeners.append(listener)
    nodes.foreach(listener)
  }

  override def registerNodeLeaveListener(listener : ServerNode => Unit): Unit = {
    nodeLeaveListeners.append(listener)
  }

  private def nodeAddHandler(path: String, node: ServerNode): Unit = {
    logger.info("Detect new node join: " + node)
    nodes.add(node)
    nodeJoinListeners.foreach(_(node))
  }

  private def nodeRemoveHandler(path: String, node: ServerNode): Unit = {
    logger.info("Detect node leave: " + node)
    nodes.remove(node)
    nodeLeaveListeners.foreach(_(node))
  }
}
