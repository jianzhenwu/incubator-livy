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

import scala.reflect.ClassTag

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.apache.curator.test.TestingServer
import org.mockito.Mockito.when
import org.scalatest.FunSpec
import org.scalatest.Matchers.{be, convertToAnyShouldWrapper, startWith}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf, ServerMetadata}
import org.apache.livy.server.recovery.{SessionStore, ZooKeeperManager, ZooKeeperStateStore}
import org.apache.livy.sessions.Session.RecoveryMetadata

@JsonIgnoreProperties(ignoreUnknown = true)
case class MockRecoveryMetadata(
    id: Int,
    name: String,
    serverMetadata: ServerMetadata,
    version: Int = 1) extends RecoveryMetadata

class MockSessionAllocator(
    livyConf: LivyConf,
    clusterManager: ClusterManager,
    sessionStore: SessionStore)
  extends SessionAllocator(livyConf, clusterManager, sessionStore) {
  override def findServer[T <: RecoveryMetadata](
      sessionType: String,
      sessionId: Int)(implicit t: ClassTag[T]): Option[ServerNode] = {
    None
  }

  override def allocateServer[T <: RecoveryMetadata](
      sessionType: String,
      sessionId: Int)(implicit t: ClassTag[T]): ServerNode = {
    null
  }

  override def onServerJoin(serverNode: ServerNode): Unit = {
  }

  override def onServerLeave(serverNode: ServerNode): Unit = {
  }
}
class SessionAllocatorSpec extends FunSpec with LivyBaseUnitTestSuite {

  describe("SessionAllocator") {
    it("should create SessionAllocator by configuration") {
      val livyConf = new LivyConf()
      livyConf.set(LivyConf.CLUSTER_SESSION_ALLOCATOR_CLASS,
        "org.apache.livy.cluster.MockSessionAllocator")
      val clusterManager = mock[ClusterManager]
      val sessionStore = mock[SessionStore]
      val sessionAllocator = SessionAllocator(livyConf, clusterManager, sessionStore)
      sessionAllocator.isInstanceOf[MockSessionAllocator] should be(true)
    }
  }

  describe("StateStoreMappingSessionAllocator") {
    def withTestingZkServer(
        function: (ZooKeeperManager, SessionAllocator) => Unit,
        conf: Option[LivyConf] = None): Unit = {
      val server = new TestingServer(3131)
      try {
        val livyConf = new LivyConf()
        if (conf.isDefined) {
          livyConf.setAll(conf.get)
        }
        livyConf.set(LivyConf.ZOOKEEPER_URL, "localhost:3131")
        livyConf .set(LivyConf.RECOVERY_ZK_STATE_STORE_KEY_PREFIX, "livy/sessions")

        val clusterManager = mock[ClusterManager]

        val serverNode1 = new ServerNode("127.0.0.1", 8999, System.currentTimeMillis())
        val serverNode2 = new ServerNode("127.0.0.2", 8999, System.currentTimeMillis())
        when(clusterManager.getNodes()).thenReturn(Set(serverNode1, serverNode2))

        val zkManager = new ZooKeeperManager(livyConf)
        zkManager.start()
        val stateStore = new ZooKeeperStateStore(livyConf, zkManager)
        val sessionStore = new SessionStore(livyConf, stateStore)

        val sessionAllocator =
          new StateStoreMappingSessionAllocator(livyConf, clusterManager, sessionStore)

        function(zkManager, sessionAllocator)
      } finally {
        server.stop()
      }
    }

    it("findServer should return correct server node") {
      withTestingZkServer((zkManager, sessionAllocator) => {
        val mockRecoveryMetadata = new MockRecoveryMetadata(1, "dummy-name",
          new ServerMetadata("127.0.0.1", 8999))
        zkManager.set("/livy/sessions/v1/dummy-type/1", mockRecoveryMetadata)

        val serverNode = sessionAllocator.findServer[MockRecoveryMetadata]("dummy-type", 1)
        serverNode.get.host should be ("127.0.0.1")
        serverNode.get.port should be (8999)
      })
    }

    it("findServer should return even when server node is offline") {
      withTestingZkServer((zkManager, sessionAllocator) => {
        val mockRecoveryMetadata = new MockRecoveryMetadata(1, "dummy-name",
          new ServerMetadata("127.0.0.3", 8999))
        zkManager.set("/livy/sessions/v1/dummy-type/1", mockRecoveryMetadata)

        val serverNode = sessionAllocator.findServer[MockRecoveryMetadata]("dummy-type", 1)
        serverNode.get.host should be ("127.0.0.3")
        serverNode.get.port should be (8999)
      })
    }

    it("findServer should return None when session not found in session store ") {
      withTestingZkServer((zkManager, sessionAllocator) => {
        val serverNode = sessionAllocator.findServer[MockRecoveryMetadata]("dummy-type", 1)
        serverNode should be(None)
      })
    }

    it("allocateServer random should work") {
      val livyConf = new LivyConf()
      livyConf.set(LivyConf.CLUSTER_SESSION_ALLOCATOR_STATE_STORE_ALGO, "RANDOM")
      withTestingZkServer((zkManager, sessionAllocator) => {
        val serverNode = sessionAllocator.allocateServer[MockRecoveryMetadata]("dummy-type", 1)
        serverNode.host should startWith("127.0.0.")
        serverNode.port should be(8999)
      }, Option(livyConf))
    }

    it("allocateServer hash should should work") {
      val livyConf = new LivyConf()
      livyConf.set(LivyConf.CLUSTER_SESSION_ALLOCATOR_STATE_STORE_ALGO, "HASH")
      withTestingZkServer((zkManager, sessionAllocator) => {
        val serverNode1 = sessionAllocator.allocateServer[MockRecoveryMetadata]("dummy-type", 1)
        serverNode1.host should be("127.0.0.2")
        serverNode1.port should be(8999)

        val serverNode2 = sessionAllocator.allocateServer[MockRecoveryMetadata]("dummy-type", 4)
        serverNode2.host should be("127.0.0.1")
        serverNode2.port should be(8999)
      }, Option(livyConf))
    }

    it("allocateServer round-robin should work") {
      val livyConf = new LivyConf()
      livyConf.set(LivyConf.CLUSTER_SESSION_ALLOCATOR_STATE_STORE_ALGO, "ROUND-ROBIN")
      withTestingZkServer((zkManager, sessionAllocator) => {
        val serverNode1 = sessionAllocator.allocateServer[MockRecoveryMetadata]("dummy-type", 1)
        serverNode1.host should be("127.0.0.1")
        serverNode1.port should be(8999)

        val serverNode2 = sessionAllocator.allocateServer[MockRecoveryMetadata]("dummy-type", 2)
        serverNode2.host should be("127.0.0.2")
        serverNode2.port should be(8999)
      }, Option(livyConf))
    }

    it("allocateServer should create state store") {
      val livyConf = new LivyConf()
      livyConf.set(LivyConf.CLUSTER_SESSION_ALLOCATOR_STATE_STORE_ALGO, "HASH")
      withTestingZkServer((zkManager, sessionAllocator) => {
        val serverNode1 = sessionAllocator.allocateServer[MockRecoveryMetadata]("dummy-type", 1)
        serverNode1.host should be("127.0.0.2")
        serverNode1.port should be(8999)

        val updatedR = zkManager.get[MockRecoveryMetadata]("/livy/sessions/v1/dummy-type/1")
        updatedR.get.serverMetadata.host should be("127.0.0.2")
        updatedR.get.serverMetadata.port should be(8999)
      }, Option(livyConf))
    }

    it("allocateServer should overwrite state store") {
      val livyConf = new LivyConf()
      livyConf.set(LivyConf.CLUSTER_SESSION_ALLOCATOR_STATE_STORE_ALGO, "HASH")
      withTestingZkServer((zkManager, sessionAllocator) => {
        val mockRecoveryMetadata = new MockRecoveryMetadata(1, "dummy-name",
          new ServerMetadata("127.0.0.1", 8999))
        zkManager.set("/livy/sessions/v1/dummy-type/1", mockRecoveryMetadata)

        val serverNode1 = sessionAllocator.allocateServer[MockRecoveryMetadata]("dummy-type", 1)
        serverNode1.host should be("127.0.0.2")
        serverNode1.port should be(8999)

        val updatedR = zkManager.get[MockRecoveryMetadata]("/livy/sessions/v1/dummy-type/1")
        updatedR.get.serverMetadata.host should be("127.0.0.2")
        updatedR.get.serverMetadata.port should be(8999)
      }, Option(livyConf))
    }

  }
}
