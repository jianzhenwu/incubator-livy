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

import org.apache.curator.test.TestingServer
import org.mockito.Matchers.anyString
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{FunSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.server.recovery.{SessionManagerState, SessionStore, ZooKeeperManager}

class SessionIdGeneratorSpec extends FunSpec with Matchers with LivyBaseUnitTestSuite {

  describe("SessionIdGenerator") {
    it("should create SessionIdGenerator by LivyConf") {
      val livyConf = new LivyConf()
        .set(LivyConf.SESSION_ID_GENERATOR_CLASS,
          "org.apache.livy.sessions.InMemorySessionIdGenerator")
      val sessionStore = mock[SessionStore]
      val sessionIdGenerator = SessionIdGenerator(livyConf, sessionStore, None, None)
      sessionIdGenerator.getClass should be(classOf[InMemorySessionIdGenerator])
    }
  }

  describe("InMemorySessionIdGenerator") {
    it("session id should increment based on different type") {
      val livyConf = new LivyConf()
      val sessionStore = mock[SessionStore]
      when(sessionStore.getNextSessionId(anyString())).thenReturn(0)

      val sessionIdGenerator = new InMemorySessionIdGenerator(livyConf, sessionStore, None, None)
      sessionIdGenerator.nextId("type1") should be(0)
      sessionIdGenerator.nextId("type2") should be(0)
      sessionIdGenerator.nextId("type1") should be(1)
      sessionIdGenerator.nextId("type2") should be(1)
    }
  }

  describe("ZookeeperStateStoreSessionIdGenerator") {
    it("session id should increment based on different type") {
      val server = new TestingServer(3131)
      try {
        val livyConf = new LivyConf()
          .set(LivyConf.ZOOKEEPER_URL, "localhost:3131")
          .set(LivyConf.RECOVERY_STATE_STORE, "zookeeper")
        val sessionStore = mock[SessionStore]
        when(sessionStore.sessionManagerPath(anyString())).thenAnswer(
          new Answer[String]() {
            override def answer(invocation: InvocationOnMock): String = {
              val args = invocation.getArguments
              val sessionType = args(0).asInstanceOf[String]
              s"$sessionType/state"
            }
          })
        val zkManager = new ZooKeeperManager(livyConf)
        zkManager.start()

        val sessionIdGenerator = new ZookeeperStateStoreSessionIdGenerator(livyConf, sessionStore,
          None, Option(zkManager))

        sessionIdGenerator.nextId("type1") should be(0)
        sessionIdGenerator.nextId("type2") should be(0)
        sessionIdGenerator.nextId("type1") should be(1)
        sessionIdGenerator.nextId("type2") should be(1)

        zkManager.get[SessionManagerState]("/livy/type1/state").get.nextSessionId should be(2)
        zkManager.get[SessionManagerState]("/livy/type2/state").get.nextSessionId should be(2)

        zkManager.stop()
      } finally {
        server.stop()
      }
    }
  }

}
