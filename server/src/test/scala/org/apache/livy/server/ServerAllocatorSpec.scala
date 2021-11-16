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

package org.apache.livy.server

import org.mockito.Mockito.when
import org.scalatest.FunSpec
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf, ServerMetadata}
import org.apache.livy.cluster.{ClusterManager, ServerAllocator, ServerNode}
import org.apache.livy.server.batch.BatchRecoveryMetadata
import org.apache.livy.server.interactive.InteractiveRecoveryMetadata

class ServerAllocatorSpec extends FunSpec with LivyBaseUnitTestSuite {

  describe("LivyConf ServerMapping") {
    it("should map the servers to users") {
      val livyConf = new LivyConf()
      livyConf.set("livy.server.mapping.user1", "10.10.10.1:8998, 10.10.10.2:8998")
      livyConf.set("livy.server.mapping.user2", "10.10.10.3:8998, 10.10.10.2:8998")
      assert(livyConf.serverMapping.size == 2,
        "The size of serverMapping should 2")
      assert(livyConf.serverMapping.contains("user1"),
        "The serverMapping should contains user1")
      assert(livyConf.serverMapping("user2").contains("10.10.10.3:8998"),
        "The user user2 should has the server 10.10.10.3:8998")
    }

    it("should return empty serverMapping") {
      val livyConf = new LivyConf()
      assert(livyConf.serverMapping.isEmpty, "The serverMapping should be empty")
    }

    it("should throw assert error if missing owner") {
      val livyConf = new LivyConf()
      livyConf.set("livy.server.mapping", "10.10.10.1:8998, 10.10.10.2:8998")
      assertThrows[AssertionError] {
        livyConf.serverMapping
      }
    }
  }

  describe("ServerAllocator") {
    it("should allocate servers by user") {
      val livyConf = new LivyConf()
      livyConf.set("livy.server.mapping.user1", "10.10.10.1:8998, 10.10.10.2:8998")
      livyConf.set("livy.server.mapping.user2", "10.10.10.3:8998, 10.10.10.2:8998")

      val clusterManager = mock[ClusterManager]

      val node1 = ServerNode.apply(ServerMetadata.apply("10.10.10.1", 8998), 1L)
      val node2 = ServerNode.apply(ServerMetadata.apply("10.10.10.2", 8998), 1L)
      val node3 = ServerNode.apply(ServerMetadata.apply("10.10.10.3", 8998), 1L)
      val node4 = ServerNode.apply(ServerMetadata.apply("10.10.10.4", 8998), 1L)
      when(clusterManager.getNodes())
        .thenReturn(Set(node1, node2, node3, node4))

      val serverAllocator = new ServerAllocator(livyConf, clusterManager)
      val serverAllocated = serverAllocator
        .allocateServer[BatchRecoveryMetadata]("batches", 0, "user1")

      assert(serverAllocated.size == 2,
        "The size of servers allocated to user1 should be 2.")
      assert(serverAllocated.contains(node1),
        "The servers allocated to user1 should contains node1.")

      val serverAllocated2 = serverAllocator
        .allocateServer[InteractiveRecoveryMetadata]("sessions", 0, "user3")
      assert(serverAllocated2.size == 1,
        "The size of servers allocated to user3 should be 1.")
      assert(serverAllocated2.head.equals(node4),
        "The server allocated to user3 should be node4")
    }
  }
}
