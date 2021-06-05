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

import java.util
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.{ProtectACLCreateModePathAndBytesable, _}
import org.apache.curator.framework.listen.{Listenable, ListenerContainer}
import org.apache.curator.framework.recipes.cache.{ChildData, PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.zookeeper.data.Stat
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.{anyObject, anyString}
import org.mockito.Mockito.{doNothing, verify, when}
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.server.recovery.ZooKeeperManager

class ZookeeperClusterManagerSpec extends FunSpec with LivyBaseUnitTestSuite {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val defaultZkServerRegisterKeyPrefix = "/livy/server"

  describe("ZookeeperClusterManager") {
    case class TestFixture(
                            conf: LivyConf,
                            zkManager: ZooKeeperManager,
                            curatorClient: CuratorFramework,
                            listenerCapture: ArgumentCaptor[PathChildrenCacheListener])

    def withMock[R](testBody: TestFixture => R): R = {
      val curatorClient = mock[CuratorFramework]
      when(curatorClient.getUnhandledErrorListenable())
        .thenReturn(mock[Listenable[UnhandledErrorListener]])

      val conf = new LivyConf()
      // conf.set(LivyConf.HA_MODE, LivyConf.HA_MODE_MULTI_ACTIVE)
      // conf.set(LivyConf.RECOVERY_STATE_STORE_URL, "host")
      conf.set(LivyConf.SERVER_HOST, "host")
      conf.set(LivyConf.CLUSTER_ZK_SERVER_REGISTER_KEY_PREFIX, defaultZkServerRegisterKeyPrefix)

      val listenerCapture = ArgumentCaptor.forClass(
        Class.forName("org.apache.curator.framework.recipes.cache.PathChildrenCacheListener")
          .asInstanceOf[Class[PathChildrenCacheListener]])
      val zkManager = new ZooKeeperManager(conf, Some(curatorClient)) {
        override protected def getPathChildrenCache(path: String): PathChildrenCache = {
          val childrenCache = mock[PathChildrenCache]
          val listenable = mock[ListenerContainer[PathChildrenCacheListener]]
          when(childrenCache.getListenable).thenReturn(listenable)
          doNothing().when(listenable).addListener(listenerCapture.capture())
          childrenCache
        }
      }
      zkManager.start()
      testBody(TestFixture(conf, zkManager, curatorClient, listenerCapture))
    }

    def mockEmptyServices(curatorClient: CuratorFramework): Unit = {
      val existsBuilder = mock[ExistsBuilder]
      when(curatorClient.checkExists()).thenReturn(existsBuilder)
    }

    def mockExistingServices(curatorClient: CuratorFramework): Unit = {
      val existsBuilder = mock[ExistsBuilder]
      when(curatorClient.checkExists()).thenReturn(existsBuilder)
      val stat = mock[Stat]
      when(existsBuilder.forPath(anyString())).thenReturn(stat)
      val getChildrenBuilder = mock[GetChildrenBuilder]
      when(curatorClient.getChildren).thenReturn(getChildrenBuilder)
      val nodeList = new util.ArrayList[String]()
      nodeList.add("host1:8998")
      nodeList.add("host2:8999")
      when(getChildrenBuilder.forPath(defaultZkServerRegisterKeyPrefix)).thenReturn(nodeList)

      val getDataBuilder = mock[GetDataBuilder]
      when(curatorClient.getData).thenReturn(getDataBuilder)

      when(getDataBuilder.forPath(s"$defaultZkServerRegisterKeyPrefix/host1:8998"))
        .thenReturn(generateNodeBytes("host1", 8998))
      when(getDataBuilder.forPath(s"$defaultZkServerRegisterKeyPrefix/host2:8999"))
        .thenReturn(generateNodeBytes("host2", 8999))
    }

    def mockCreateEphemeralNode(
        curatorClient: CuratorFramework): ACLBackgroundPathAndBytesable[String] = {
      val createBuilder = mock[CreateBuilder]
      when(curatorClient.create()).thenReturn(createBuilder)

      val creator = mock[ProtectACLCreateModePathAndBytesable[String]]
      when(createBuilder.creatingParentsIfNeeded()).thenReturn(creator)

      val path = mock[ACLBackgroundPathAndBytesable[String]]
      when(creator.withMode(anyObject())).thenReturn(path)

      path
    }

    it("should return correct nodes list") {
      withMock { f =>
        mockEmptyServices(f.curatorClient)
        var zkClusterManager = new ZookeeperClusterManager(f.conf, f.zkManager)
        zkClusterManager.getNodes().size shouldBe 0

        mockExistingServices(f.curatorClient)
        zkClusterManager = new ZookeeperClusterManager(f.conf, f.zkManager)
        val nodeList = zkClusterManager.getNodes().toList.sortWith(_.port < _.port)
        nodeList.size shouldBe 2
        nodeList(0).host shouldBe "host1"
        nodeList(0).port shouldBe 8998
        nodeList(1).host shouldBe "host2"
        nodeList(1).port shouldBe 8999
      }
    }

    it("register should use curatorClient") {
      withMock { f =>
        mockEmptyServices(f.curatorClient)
        val path = mockCreateEphemeralNode(f.curatorClient)
        val zkClusterManager = new ZookeeperClusterManager(f.conf, f.zkManager)

        zkClusterManager.register()
        val dir = ArgumentCaptor.forClass("".getClass)
        val data = ArgumentCaptor.forClass(new Array[Byte](0).getClass)
        verify(path).forPath(dir.capture(), data.capture())

        dir.getValue shouldBe s"$defaultZkServerRegisterKeyPrefix/host:8998"
        val node = mapper.readValue(data.getValue, classOf[ServiceNode])
        node.host shouldBe "host"
        node.port shouldBe 8998
      }
    }

    it("register node join listener") {
      withMock { f =>
        mockEmptyServices(f.curatorClient)
        val zkClusterManager = new ZookeeperClusterManager(f.conf, f.zkManager)

        var counter = 0
        zkClusterManager.registerNodeJoinListener(f => {
          counter += 1
        })

        val childData = new ChildData(
          s"$defaultZkServerRegisterKeyPrefix/host1:8998",
          mock[Stat],
          generateNodeBytes("host1", 8998))
        f.listenerCapture.getAllValues.get(0).childEvent(
          f.curatorClient,
          new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_ADDED, childData))

        counter shouldBe 1
        val nodeList = zkClusterManager.getNodes().toList
        nodeList.size shouldBe 1
        nodeList(0).host shouldBe "host1"
        nodeList(0).port shouldBe 8998
      }
    }

    it("register node leave listener") {
      withMock { f =>
        mockExistingServices(f.curatorClient)
        val zkClusterManager = new ZookeeperClusterManager(f.conf, f.zkManager)
        var nodeList = zkClusterManager.getNodes().toList
        val nodeToBeDelete = nodeList(0)

        var counter = 0
        zkClusterManager.registerNodeLeaveListener(f => {
          counter += 1
        })

        val childData = new ChildData(
          s"$defaultZkServerRegisterKeyPrefix/host1:8998",
          mock[Stat],
          mapper.writeValueAsBytes(nodeToBeDelete))
        f.listenerCapture.getAllValues.get(1).childEvent(
          f.curatorClient,
          new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED, childData))

        counter shouldBe 1
        nodeList = zkClusterManager.getNodes().toList
        nodeList.size shouldBe 1
      }
    }
  }

  private def generateNodeBytes(host: String, port: Int): Array[Byte] = {
    mapper.writeValueAsBytes(new ServiceNode(host, port, UUID.randomUUID().toString))
  }
}
