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

package org.apache.livy.client.http

import java.net.{InetAddress, URI}
import java.util.Properties
import javax.servlet.ServletContext
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable

import org.mockito.Mockito.{mock, when}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike}
import org.scalatra.LifeCycle
import org.scalatra.servlet.ScalatraListener
import scala.concurrent.{ExecutionContext, Future}

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf, MasterMetadata, ServerMetadata}
import org.apache.livy.client.http.param.BatchOptions
import org.apache.livy.metrics.common.Metrics
import org.apache.livy.server.{AccessManager, WebServer}
import org.apache.livy.server.batch.{BatchRecoveryMetadata, BatchSession, BatchSessionServlet, BatchSessionView}
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.{BatchSessionManager, SessionIdGenerator, SessionState}
import org.apache.livy.utils.AppInfo

class BatchRestClientSpec extends FunSpecLike
  with BeforeAndAfterAll
  with LivyBaseUnitTestSuite {

  private var server: WebServer = _
  private var client: BatchRestClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    Metrics.init(new LivyConf())
    server = new WebServer(new LivyConf(), "0.0.0.0", 0)

    server.context.setResourceBase("src/main/org/apache/livy/server")
    server.context.setInitParameter(ScalatraListener.LifeCycleKey,
      classOf[RestClientBatchTestBootstrap].getCanonicalName)
    server.context.addEventListener(new ScalatraListener)

    server.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (client != null) {
      client.stop(true)
      client = null
    }
    if (server != null) {
      server.stop()
      server = null
    }
  }

  describe("Rest client batch session") {

    it("should create batch session and run job") {
      // WebServer does this internally instead of respecting "0.0.0.0", so try to use the same
      // address.
      val uri = s"http://${InetAddress.getLocalHost.getHostAddress}:${server.port}/"
      client = new BatchRestClient(new URI(uri), new Properties())
      val options: BatchOptions = new BatchOptions()
      val batchSessionView = client.submitBatchJob(options)
      assert(batchSessionView.getId === 0)
      assert(batchSessionView.getState.equals(SessionState.Idle.state))
    }
  }
}

private class RestClientBatchTestBootstrap extends LifeCycle {

  private implicit def executor: ExecutionContext = ExecutionContext.global

  override def init(context: ServletContext): Unit = {
    val conf = new LivyConf()
    val masterMetadata = mock(classOf[MasterMetadata])
    val stateStore = mock(classOf[SessionStore])
    val sessionIdGenerator = mock(classOf[SessionIdGenerator])
    val sessionManager = new BatchSessionManager(conf, stateStore, sessionIdGenerator,
      Some(Seq.empty))
    val accessManager = new AccessManager(conf)
    val servlet = new BatchSessionServlet(sessionManager, None, None, stateStore, conf,
      accessManager) {
      override protected def createSession(
                                            sessionId: Int,
                                            req: HttpServletRequest): BatchSession = {
        val session = mock(classOf[BatchSession])
        val id = sessionManager.nextId()
        when(session.id).thenReturn(id)
        when(session.name).thenReturn(None)
        when(session.appId).thenReturn(None)
        when(session.appInfo).thenReturn(AppInfo())
        when(session.state).thenReturn(SessionState.Idle)
        when(session.proxyUser).thenReturn(None)

        when(session.recoveryMetadata).thenReturn(BatchRecoveryMetadata(id, None,
          None, null, null, None, livyConf.serverMetadata(), masterMetadata))
        when(session.stop()).thenReturn(Future.successful(()))

        when(session.recoveryMetadata).thenReturn(
          BatchRecoveryMetadata(0, None, None, "",
            "", None, ServerMetadata(req.serverName, req.serverPort), masterMetadata))
        when(session.optimizedConf).thenReturn(Some(mutable.Map[String, AnyRef]()))

        require(HttpClientSpec.session == null, "Session already created?")
        session
      }
      override protected[livy] def clientSessionView(session: BatchSession,
          req: HttpServletRequest): Any = {

        BatchSessionView(session.id, session.name, session.owner, session.proxyUser,
          session.state.toString, session.appId, session.appInfo, Nil,
          Option(session.recoveryMetadata.serverMetadata.toString()))
      }
      override protected[livy] def clientSessionCreationView(session: BatchSession,
          req: HttpServletRequest): Any = {
        clientSessionView(session, req)
      }

    }
    context.mount(servlet, s"/${SessionType.Batches.getSessionType}/*")
  }

}
