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

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.Try

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import org.apache.curator.test.TestingServer
import org.mockito.Matchers.anyObject
import org.mockito.Mockito.{times, verify, when}
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
    version: Int = 1) extends RecoveryMetadata {
  @JsonIgnore
  override def isServerDeallocatable(): Boolean = { true }
  @JsonIgnore
  override def isRecoverable(): Boolean = { false }
}

class MockSessionAllocator(
    livyConf: LivyConf,
    clusterManager: ClusterManager,
    sessionStore: SessionStore,
    zkManager: ZooKeeperManager)
  extends SessionAllocator(livyConf, clusterManager, sessionStore, zkManager) {
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

  override def deallocateServer[T <: RecoveryMetadata](
      sessionType: String,
      sessionId: Int)(implicit t: ClassTag[T]): Unit = {
    // Do nothing
  }

  override def onServerJoin(serverNode: ServerNode): Unit = {
  }

  override def onServerLeave(serverNode: ServerNode): Unit = {
  }

  override def getAllSessions[T <: RecoveryMetadata : ClassTag](
      sessionType: String,
      serverMetadata: Option[ServerMetadata]): Seq[Try[T]] = {
    Seq.empty
  }
}

object MockStateStoreMappingSessionAllocator {
  val serverNodes = ListBuffer[ServerNode]()
}

class MockStateStoreMappingSessionAllocator(
     livyConf: LivyConf,
     clusterManager: ClusterManager,
     sessionStore: SessionStore,
     zkManager: ZooKeeperManager)
  extends StateStoreMappingSessionAllocator(livyConf, clusterManager, sessionStore, zkManager) {

  override def onServerJoin(serverNode: ServerNode): Unit = {
    MockStateStoreMappingSessionAllocator.serverNodes += serverNode
  }

  override def onServerLeave(serverNode: ServerNode): Unit = {
    MockStateStoreMappingSessionAllocator.serverNodes -= serverNode
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
      val zkManager = mock[ZooKeeperManager]
      val sessionAllocator = SessionAllocator(livyConf, clusterManager, sessionStore, zkManager)
      sessionAllocator.isInstanceOf[MockSessionAllocator] should be(true)

      verify(clusterManager, times(1)).registerNodeJoinListener(anyObject())
      verify(clusterManager, times(1)).registerNodeLeaveListener(anyObject())
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
        livyConf.set(LivyConf.RECOVERY_ZK_STATE_STORE_KEY_PREFIX, "livy/sessions")

        val zkManager = new ZooKeeperManager(livyConf)
        zkManager.start()
        val stateStore = new ZooKeeperStateStore(livyConf, zkManager)
        val sessionStore = new SessionStore(livyConf, stateStore)

        val serverNode1 = new ServerNode("126.0.0.1", 8999, System.currentTimeMillis())
        val serverNode2 = new ServerNode("126.0.0.2", 8999, System.currentTimeMillis() + 1000)

        zkManager.set("/livy/servers/126.0.0.1:8999", serverNode1)
        zkManager.set("/livy/servers/126.0.0.2:8999", serverNode2)

        val clusterManager = new ZookeeperClusterManager(livyConf, zkManager)

        livyConf.set(LivyConf.CLUSTER_SESSION_ALLOCATOR_CLASS,
          "org.apache.livy.cluster.MockStateStoreMappingSessionAllocator")
        val sessionAllocator = SessionAllocator(livyConf, clusterManager, sessionStore, zkManager)

        function(zkManager, sessionAllocator)
      } finally {
        server.stop()
      }
    }

    it("findServer should return correct server node") {
      withTestingZkServer((zkManager, sessionAllocator) => {
        val mockRecoveryMetadata = new MockRecoveryMetadata(1, "dummy-name",
          new ServerMetadata("126.0.0.1", 8999))
        zkManager.set("/livy/sessions/v1/dummy-type/1", mockRecoveryMetadata)

        val serverNode = sessionAllocator.findServer[MockRecoveryMetadata]("dummy-type", 1)
        serverNode.get.host should be ("126.0.0.1")
        serverNode.get.port should be (8999)
      })
    }

    it("findServer should return even when server node is offline") {
      withTestingZkServer((zkManager, sessionAllocator) => {
        val mockRecoveryMetadata = new MockRecoveryMetadata(1, "dummy-name",
          new ServerMetadata("126.0.0.3", 8999))
        zkManager.set("/livy/sessions/v1/dummy-type/1", mockRecoveryMetadata)

        val serverNode = sessionAllocator.findServer[MockRecoveryMetadata]("dummy-type", 1)
        serverNode.get.host should be ("126.0.0.3")
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
        serverNode.host should startWith("126.0.0.")
        serverNode.port should be(8999)
      }, Option(livyConf))
    }

    it("allocateServer hash should should work") {
      val livyConf = new LivyConf()
      livyConf.set(LivyConf.CLUSTER_SESSION_ALLOCATOR_STATE_STORE_ALGO, "HASH")
      withTestingZkServer((zkManager, sessionAllocator) => {
        val serverNode1 = sessionAllocator.allocateServer[MockRecoveryMetadata]("dummy-type", 1)
        serverNode1.host should be("126.0.0.2")
        serverNode1.port should be(8999)

        val serverNode2 = sessionAllocator.allocateServer[MockRecoveryMetadata]("dummy-type", 4)
        serverNode2.host should be("126.0.0.1")
        serverNode2.port should be(8999)

        serverNode1.host shouldNot be(serverNode2.host)
      }, Option(livyConf))
    }

    it("allocateServer round-robin should work") {
      val livyConf = new LivyConf()
      livyConf.set(LivyConf.CLUSTER_SESSION_ALLOCATOR_STATE_STORE_ALGO, "ROUND-ROBIN")
      withTestingZkServer((zkManager, sessionAllocator) => {
        val serverNode1 = sessionAllocator.allocateServer[MockRecoveryMetadata]("dummy-type", 1)
        serverNode1.host should be("126.0.0.1")
        serverNode1.port should be(8999)

        val serverNode2 = sessionAllocator.allocateServer[MockRecoveryMetadata]("dummy-type", 2)
        serverNode2.host should be("126.0.0.2")
        serverNode2.port should be(8999)

        serverNode1.host shouldNot be(serverNode2.host)
      }, Option(livyConf))
    }

    it("allocateServer should create state store") {
      val livyConf = new LivyConf()
      livyConf.set(LivyConf.CLUSTER_SESSION_ALLOCATOR_STATE_STORE_ALGO, "HASH")
      withTestingZkServer((zkManager, sessionAllocator) => {
        val serverNode1 = sessionAllocator.allocateServer[MockRecoveryMetadata]("dummy-type", 1)
        serverNode1.host should be("126.0.0.2")
        serverNode1.port should be(8999)

        val updatedR = zkManager.get[MockRecoveryMetadata]("/livy/sessions/v1/dummy-type/1")
        updatedR.get.serverMetadata.host should be(serverNode1.host)
        updatedR.get.serverMetadata.port should be(8999)
      }, Option(livyConf))
    }

    it("allocateServer should return current server if it is online") {
      val livyConf = new LivyConf()
      livyConf.set(LivyConf.CLUSTER_SESSION_ALLOCATOR_STATE_STORE_ALGO, "HASH")
      withTestingZkServer((zkManager, sessionAllocator) => {
        val mockRecoveryMetadata = new MockRecoveryMetadata(1, "dummy-name",
          new ServerMetadata("126.0.0.1", 8999))
        zkManager.set("/livy/sessions/v1/dummy-type/1", mockRecoveryMetadata)

        val serverNode1 = sessionAllocator.allocateServer[MockRecoveryMetadata]("dummy-type", 1)
        serverNode1.host should be("126.0.0.1")
        serverNode1.port should be(8999)

        val updatedR = zkManager.get[MockRecoveryMetadata]("/livy/sessions/v1/dummy-type/1")
        updatedR.get.serverMetadata.host should be("126.0.0.1")
        updatedR.get.serverMetadata.port should be(8999)
      }, Option(livyConf))
    }

    it("allocateServer should return new server if it is offline") {
      val livyConf = new LivyConf()
      livyConf.set(LivyConf.CLUSTER_SESSION_ALLOCATOR_STATE_STORE_ALGO, "HASH")
      withTestingZkServer((zkManager, sessionAllocator) => {
        val mockRecoveryMetadata = new MockRecoveryMetadata(1, "dummy-name",
          new ServerMetadata("126.0.0.3", 8999))
        zkManager.set("/livy/sessions/v1/dummy-type/1", mockRecoveryMetadata)

        val serverNode1 = sessionAllocator.allocateServer[MockRecoveryMetadata]("dummy-type", 1)
        serverNode1.host should be("126.0.0.2")
        serverNode1.port should be(8999)

        val updatedR = zkManager.get[MockRecoveryMetadata]("/livy/sessions/v1/dummy-type/1")
        updatedR.get.serverMetadata.host should be(serverNode1.host)
        updatedR.get.serverMetadata.port should be(8999)
      }, Option(livyConf))
    }

    it("should be notified when server join or leave") {
      val serverNodes = MockStateStoreMappingSessionAllocator.serverNodes
      serverNodes.clear()

      withTestingZkServer((zkManager, sessionAllocator) => {
        val serverNode3 = new ServerNode("126.0.0.3", 8999, System.currentTimeMillis())

        zkManager.set("/livy/servers/126.0.0.3:8999", serverNode3)
        Thread.sleep(100)
        serverNodes.size should be(3)
        zkManager.remove("/livy/servers/126.0.0.3:8999")
        Thread.sleep(100)
        serverNodes.size should be(2)
      })
    }

    it("getAllSessions should return all session in cluster") {
      withTestingZkServer((zkManager, sessionAllocator) => {
        val mockRecoveryMetadata1 = new MockRecoveryMetadata(1, "dummy-name",
          new ServerMetadata("126.0.0.1", 8999))
        zkManager.set("/livy/sessions/v1/dummy-type/1", mockRecoveryMetadata1)

        val mockRecoveryMetadata2 = new MockRecoveryMetadata(2, "dummy-name",
          new ServerMetadata("126.0.0.2", 8999))
        zkManager.set("/livy/sessions/v1/dummy-type/2", mockRecoveryMetadata2)

        val sessions = sessionAllocator.getAllSessions[MockRecoveryMetadata]("dummy-type", None)
        sessions.size should be(2)

        var s1 = sessions.head.get
        var s2 = sessions(1).get
        if (s1.id != 1) {
          val tmp = s1
          s1 = s2
          s2 = tmp
        }
        s1.id should be(1)
        s1.name should be("dummy-name")
        s1.serverMetadata.host should be("126.0.0.1")
        s1.serverMetadata.port should be(8999)

        s2.id should be(2)
        s2.name should be("dummy-name")
        s2.serverMetadata.host should be("126.0.0.2")
        s2.serverMetadata.port should be(8999)
      })
    }
  }
}
