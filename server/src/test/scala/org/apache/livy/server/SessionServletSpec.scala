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

import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse._

import scala.collection.mutable.ArrayBuffer
import scala.util.Success

import com.squareup.okhttp.{CacheControl, Request}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.livy.{LivyConf, ServerMetadata}
import org.apache.livy.cluster.{ClusterManager, ServerNode, SessionAllocator}
import org.apache.livy.server.SessionServletSpec.{MockRecoveryMetadata, MockSession, MockSessionView, MockSessionViews}
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.{Session, SessionIdGenerator, SessionManager, SessionState}
import org.apache.livy.sessions.Session.RecoveryMetadata

object SessionServletSpec {

  val PROXY_USER = "proxyUser"

  case class MockRecoveryMetadata(
       id: Int,
       serverMetadata: ServerMetadata) extends RecoveryMetadata

  class MockSession(id: Int, owner: String, val proxyUser: Option[String], livyConf: LivyConf)
    extends Session(id, None, owner, livyConf) {

    override def recoveryMetadata: RecoveryMetadata =
      MockRecoveryMetadata(0, livyConf.serverMetadata())

    override def state: SessionState = SessionState.Idle

    override def start(): Unit = ()

    override protected def stopSession(): Unit = ()

    override def logLines(): IndexedSeq[String] = IndexedSeq("log")
  }

  case class MockSessionView(id: Int, owner: String, proxyUser: Option[String], logs: Seq[String])

  case class MockSessionViews(total: Int, from: Int, sessions: Seq[MockSessionView])

  def createServlet(conf: LivyConf): SessionServlet[Session, RecoveryMetadata] = {
    val sessionManager = new SessionManager[Session, RecoveryMetadata](
      conf,
      { _ => assert(false).asInstanceOf[Session] },
      mock[SessionStore],
      "test",
      mock[SessionIdGenerator],
      Some(Seq.empty))

    val accessManager = new AccessManager(conf)
    new SessionServlet(sessionManager, None, None, conf, accessManager) with RemoteUserOverride {
      override protected def createSession(sessionId: Int, req: HttpServletRequest): Session = {
        val params = bodyAs[Map[String, String]](req)
        val owner = remoteUser(req)
        val impersonatedUser = accessManager.checkImpersonation(
          proxyUser(req, params.get(PROXY_USER)), owner)
        new MockSession(sessionId, owner, impersonatedUser, conf)
      }

      override protected def clientSessionView(
          session: Session,
          req: HttpServletRequest): Any = {
        val hasViewAccess = accessManager.hasViewAccess(session.owner,
                                                        effectiveUser(req),
                                                        session.proxyUser.getOrElse(""))
        val logs = if (hasViewAccess) {
          session.logLines()
        } else {
          Nil
        }
        MockSessionView(session.id, session.owner, session.proxyUser, logs)
      }

      override protected def filterBySearchKey(recoveryMetadata: RecoveryMetadata,
                                               searchKey: Option[String]): Boolean = {
        !searchKey.exists(_.trim.nonEmpty) || filterBySearchKey(None,
          None, recoveryMetadata.serverMetadata, searchKey.get)
      }

      override protected def filterBySearchKey(session: Session,
                                               searchKey: Option[String]): Boolean = {
        !searchKey.exists(_.trim.nonEmpty) || filterBySearchKey(session.appId,
          session.name, session.recoveryMetadata.serverMetadata, searchKey.get)
      }
    }
  }

  def createClusterEnabledServlet(conf: LivyConf)
      : SessionServlet[MockSession, MockRecoveryMetadata] = {

    val sessionStore = mock[SessionStore]
    val sessionIdGenerator = mock[SessionIdGenerator]
    when(sessionIdGenerator.isGlobalUnique()).thenReturn(true)
    val sessionManager = new SessionManager[MockSession, MockRecoveryMetadata](
      conf,
      { recoveryMetadata => new MockSession(recoveryMetadata.id, "alice", None, conf) },
      sessionStore,
      "test",
      sessionIdGenerator,
      Some(Seq(new MockSession(1, "bob", None, conf))))

    when(sessionIdGenerator.nextId(sessionManager.sessionType()))
      .thenReturn(200)
      .thenReturn(201)
      .thenReturn(202)

    val accessManager = new AccessManager(conf)
    val sessionAllocator = mock[SessionAllocator]
    val clusterManager = mock[ClusterManager]
    val serverNode127 = ServerNode(ServerMetadata("127.0.0.1", 8998))
    val serverNode128 = ServerNode(ServerMetadata("128.0.0.1", 8998))
    when(clusterManager.isNodeOnline(serverNode127)).thenReturn(true)
    when(clusterManager.isNodeOnline(serverNode128)).thenReturn(false)

    when(sessionStore.get[MockRecoveryMetadata](sessionManager.sessionType(), 100))
      .thenReturn(Some(MockRecoveryMetadata(100, conf.serverMetadata())))
    when(sessionAllocator.findServer[MockRecoveryMetadata](sessionManager.sessionType(), 100))
      .thenReturn(Some(ServerNode(conf.serverMetadata())))

    when(sessionAllocator.findServer[MockRecoveryMetadata](sessionManager.sessionType(), 101))
      .thenReturn(None)

    when(sessionAllocator.findServer[MockRecoveryMetadata](sessionManager.sessionType(), 102))
      .thenReturn(Some(serverNode127))

    when(sessionStore.get[MockRecoveryMetadata](sessionManager.sessionType(), 103))
      .thenReturn(Some(MockRecoveryMetadata(103, conf.serverMetadata())))
    when(sessionAllocator.findServer[MockRecoveryMetadata](sessionManager.sessionType(), 103))
      .thenReturn(Some(serverNode128))
      .thenReturn(Some(ServerNode(conf.serverMetadata())))

    when(sessionAllocator.findServer[MockRecoveryMetadata](sessionManager.sessionType(), 200))
      .thenReturn(Some(ServerNode(conf.serverMetadata())))

    when(sessionAllocator.findServer[MockRecoveryMetadata](sessionManager.sessionType(), 201))
      .thenReturn(Some(serverNode127))
      .thenReturn(Some(ServerNode(conf.serverMetadata())))

    when(sessionAllocator.findServer[MockRecoveryMetadata](sessionManager.sessionType(), 202))
      .thenReturn(Some(serverNode128))
      .thenReturn(Some(ServerNode(conf.serverMetadata())))

    when(sessionAllocator.findServer[MockRecoveryMetadata](sessionManager.sessionType(), 210))
      .thenReturn(None)

    when(sessionAllocator.getAllSessions[MockRecoveryMetadata](
      sessionManager.sessionType(), serverMetadata = None))
      .thenReturn(ArrayBuffer(Success[MockRecoveryMetadata](MockRecoveryMetadata(100,
        serverNode127.serverMetadata))))


    new SessionServlet(sessionManager,
                       Some(sessionAllocator),
                       Some(clusterManager),
                       conf,
                       accessManager) with RemoteUserOverride {

      override protected def createSession(sessionId: Int, req: HttpServletRequest): MockSession = {
        val params = bodyAs[Map[String, String]](req)
        val owner = remoteUser(req)
        val impersonatedUser = accessManager.checkImpersonation(
          proxyUser(req, params.get(PROXY_USER)), owner)
        new MockSession(sessionId, owner, impersonatedUser, conf)
      }

      override protected def clientSessionView(
          session: MockSession,
          req: HttpServletRequest): Any = {
        val hasViewAccess = accessManager.hasViewAccess(session.owner,
                                                        effectiveUser(req),
                                                        session.proxyUser.getOrElse(""))
        val logs = if (hasViewAccess) {
          session.logLines()
        } else {
          Nil
        }
        MockSessionView(session.id, session.owner, session.proxyUser, logs)
      }

      override protected def clientSessionView(recoverMetadata: MockRecoveryMetadata,
                                               req: HttpServletRequest): Any = {
        try {
          val scheme = req.getScheme
          val host = req.serverName
          val port = req.serverPort
          val path = url(getSession, "id" -> recoverMetadata.id.toString)
          val requestUrl = s"$scheme://$host:$port$path"
          val res = httpClient.newCall(new Request.Builder()
            .url(requestUrl).cacheControl(CacheControl.FORCE_NETWORK).build()).execute()

          return objectMapper.readValue(res.body().string(), classOf[MockSessionView])
        } catch {
          case e: Throwable =>
            SessionServlet.error(s"Error when executing request: ${req.getRequestURL}\n" +
              s"SessionId: ${recoverMetadata.id}\n" +
              s"Error message: ${e.getMessage}")
        }
        MockSessionView(recoverMetadata.id, "", Option(""), Seq.empty[String])
      }

      override protected def filterBySearchKey(recoveryMetadata: MockRecoveryMetadata,
                                               searchKey: Option[String]): Boolean = {
        !searchKey.exists(_.trim.nonEmpty) || filterBySearchKey(None,
          None, recoveryMetadata.serverMetadata, searchKey.get)
      }

      override protected def filterBySearchKey(session: MockSession,
                                               searchKey: Option[String]): Boolean = {
        !searchKey.exists(_.trim.nonEmpty) || filterBySearchKey(session.appId,
          session.name, session.recoveryMetadata.serverMetadata, searchKey.get)
      }
    }
  }
}

class SessionServletSpec extends BaseSessionServletSpec[Session, RecoveryMetadata] {
  import SessionServletSpec._

  override def createServlet(): SessionServlet[Session, RecoveryMetadata] = {
    SessionServletSpec.createServlet(createConf())
  }

  private val aliceHeaders = makeUserHeaders("alice")
  private val bobHeaders = makeUserHeaders("bob")

  private def delete(id: Int, headers: Map[String, String], expectedStatus: Int): Unit = {
    jdelete[Map[String, Any]](s"/$id", headers = headers, expectedStatus = expectedStatus) { _ =>
      // Nothing to do.
    }
  }

  describe("SessionServlet") {

    it("should return correct Location in header") {
      // mount to "/sessions/*" to test. If request URI is "/session", getPathInfo() will
      // return null, since there's no extra path.
      // mount to "/*" will always return "/", so that it cannot reflect the issue.
      addServlet(servlet, "/sessions/*")
      jpost[MockSessionView]("/sessions", Map(), headers = aliceHeaders) { res =>
        assert(header("Location") === "/sessions/0")
        jdelete[Map[String, Any]]("/sessions/0", SC_OK, aliceHeaders) { _ => }
      }
    }

    it("should attach owner information to sessions") {
      jpost[MockSessionView]("/", Map()) { res =>
        assert(res.owner === null)
        assert(res.proxyUser === None)
        assert(res.logs === IndexedSeq("log"))
        delete(res.id, adminHeaders, SC_OK)
      }

      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        assert(res.owner === "alice")
        assert(res.proxyUser === Some("alice"))
        assert(res.logs === IndexedSeq("log"))
        delete(res.id, aliceHeaders, SC_OK)
      }

      jpost[MockSessionView]("/?doAs=alice", Map(), headers = adminHeaders) { res =>
        assert(res.owner === ADMIN)
        assert(res.proxyUser === Some("alice"))
        assert(res.logs === IndexedSeq("log"))
        delete(res.id, aliceHeaders, SC_OK)
      }
    }

    it("should allow other users to see all information due to ACLs not enabled") {
      jpost[MockSessionView]("/", Map()) { res =>
        jget[MockSessionView](s"/${res.id}", headers = bobHeaders) { res =>
          assert(res.owner === null)
          assert(res.proxyUser === None)
          assert(res.logs === IndexedSeq("log"))
        }
        delete(res.id, adminHeaders, SC_OK)
      }

      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        jget[MockSessionView](s"/${res.id}") { res =>
          assert(res.owner === "alice")
          assert(res.proxyUser === Some("alice"))
          assert(res.logs === IndexedSeq("log"))
        }
        delete(res.id, aliceHeaders, SC_OK)
      }

      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        jget[MockSessionView](s"/${res.id}", headers = bobHeaders) { res =>
          assert(res.owner === "alice")
          assert(res.proxyUser === Some("alice"))
          assert(res.logs === IndexedSeq("log"))
        }

        jget[MockSessionView](s"/${res.id}?doAs=bob", headers = adminHeaders) { res =>
          assert(res.owner === "alice")
          assert(res.proxyUser === Some("alice"))
          assert(res.logs === IndexedSeq("log"))
        }
        delete(res.id, aliceHeaders, SC_OK)
      }

      jpost[MockSessionView]("/?doAs=alice", Map(), headers = adminHeaders) { res =>
        jget[MockSessionView](s"/${res.id}", headers = bobHeaders) { res =>
          assert(res.owner === ADMIN)
          assert(res.proxyUser === Some("alice"))
          assert(res.logs === IndexedSeq("log"))
        }
        delete(res.id, aliceHeaders, SC_OK)
      }
    }

    it("should allow non-owners to modify sessions") {
      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        delete(res.id, bobHeaders, SC_OK)
      }

      jpost[MockSessionView]("/?doAs=alice", Map(), headers = adminHeaders) { res =>
        delete(res.id, bobHeaders, SC_OK)
      }
    }

    it("should not allow regular users to impersonate others") {
      jpost[MockSessionView]("/", Map(PROXY_USER -> "bob"), headers = aliceHeaders,
        expectedStatus = SC_FORBIDDEN) { _ => }

      jpost[MockSessionView]("/?doAs=bob", Map(), headers = aliceHeaders,
        expectedStatus = SC_FORBIDDEN) { _ => }
    }

    it("should allow admins to impersonate anyone") {
      jpost[MockSessionView]("/", Map(PROXY_USER -> "bob"), headers = adminHeaders) { res =>
        delete(res.id, adminHeaders, SC_OK)
      }

      jpost[MockSessionView]("/?doAs=bob", Map(), headers = adminHeaders) { res =>
        delete(res.id, adminHeaders, SC_OK)
      }
    }
  }
}

class AclsEnabledSessionServletSpec extends BaseSessionServletSpec[Session, RecoveryMetadata] {

  import SessionServletSpec._

  override def createServlet(): SessionServlet[Session, RecoveryMetadata] = {
    val conf = createConf().set(LivyConf.ACCESS_CONTROL_ENABLED, true)
    SessionServletSpec.createServlet(conf)
  }

  private val aliceHeaders = makeUserHeaders("alice")
  private val bobHeaders = makeUserHeaders("bob")

  private def delete(id: Int, headers: Map[String, String], expectedStatus: Int): Unit = {
    jdelete[Map[String, Any]](s"/$id", headers = headers, expectedStatus = expectedStatus) { _ =>
      // Nothing to do.
    }
  }

  describe("SessionServlet") {
    it("should attach owner information to sessions") {
      jpost[MockSessionView]("/", Map()) { res =>
        assert(res.owner === null)
        assert(res.proxyUser === None)
        assert(res.logs === IndexedSeq("log"))
        delete(res.id, adminHeaders, SC_OK)
      }

      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        assert(res.owner === "alice")
        assert(res.proxyUser === Some("alice"))
        assert(res.logs === IndexedSeq("log"))
        delete(res.id, aliceHeaders, SC_OK)
      }
    }

    it("should only allow view accessible users to see non-sensitive information") {
      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        jget[MockSessionView](s"/${res.id}", headers = bobHeaders) { res =>
          assert(res.owner === "alice")
          assert(res.proxyUser === Some("alice"))
          // Other user cannot see the logs
          assert(res.logs === Nil)
        }

        jget[MockSessionView](s"/${res.id}?doAs=bob", headers = adminHeaders) { res =>
          assert(res.owner === "alice")
          assert(res.proxyUser === Some("alice"))
          // Other user cannot see the logs
          assert(res.logs === Nil)
        }

        // Users with access permission could see the logs
        jget[MockSessionView](s"/${res.id}", headers = aliceHeaders) { res =>
          assert(res.logs === IndexedSeq("log"))
        }
        jget[MockSessionView](s"/${res.id}", headers = viewUserHeaders) { res =>
          assert(res.logs === IndexedSeq("log"))
        }
        jget[MockSessionView](s"/${res.id}", headers = modifyUserHeaders) { res =>
          assert(res.logs === IndexedSeq("log"))
        }
        jget[MockSessionView](s"/${res.id}", headers = adminHeaders) { res =>
          assert(res.logs === IndexedSeq("log"))
        }

        delete(res.id, aliceHeaders, SC_OK)
      }

      jpost[MockSessionView]("/?doAs=alice", Map(), headers = adminHeaders) { res =>
        jget[MockSessionView](s"/${res.id}", headers = bobHeaders) { res =>
          assert(res.owner === ADMIN)
          assert(res.proxyUser === Some("alice"))
          // Other user cannot see the logs
          assert(res.logs === Nil)
        }

        // Users with access permission could see the logs
        jget[MockSessionView](s"/${res.id}", headers = viewUserHeaders) { res =>
          assert(res.logs === IndexedSeq("log"))
        }
        jget[MockSessionView](s"/${res.id}", headers = modifyUserHeaders) { res =>
          assert(res.logs === IndexedSeq("log"))
        }
        jget[MockSessionView](s"/${res.id}", headers = adminHeaders) { res =>
          assert(res.logs === IndexedSeq("log"))
        }

        // LIVY-592: Proxy user cannot view its session log
        // Proxy user should be able to see its session log
        jget[MockSessionView](s"/${res.id}", headers = aliceHeaders) { res =>
          assert(res.logs === IndexedSeq("log"))
        }

        delete(res.id, adminHeaders, SC_OK)
      }
    }

    it("should only allow modify accessible users from modifying sessions") {
      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        delete(res.id, bobHeaders, SC_FORBIDDEN)
        delete(res.id, viewUserHeaders, SC_FORBIDDEN)
        delete(res.id, modifyUserHeaders, SC_OK)
      }

      jpost[MockSessionView]("/?doAs=alice", Map(), headers = adminHeaders) { res =>
        delete(res.id, bobHeaders, SC_FORBIDDEN)
        delete(res.id, viewUserHeaders, SC_FORBIDDEN)
        delete(res.id, modifyUserHeaders, SC_OK)
      }

      // LIVY-592: Proxy user cannot view its session log
      // Proxy user should be able to modify its session
      jpost[MockSessionView]("/?doAs=alice", Map(), headers = adminHeaders) { res =>
        delete(res.id, aliceHeaders, SC_OK)
      }
    }

    it("should not allow regular users to impersonate others") {
      jpost[MockSessionView]("/", Map(PROXY_USER -> "bob"), headers = aliceHeaders,
        expectedStatus = SC_FORBIDDEN) { _ => }

      jpost[MockSessionView]("/?doAs=bob", Map(), headers = aliceHeaders,
        expectedStatus = SC_FORBIDDEN) { _ => }
    }

    it("should allow admins to impersonate anyone") {
      jpost[MockSessionView]("/", Map(PROXY_USER -> "bob"), headers = adminHeaders) { res =>
        delete(res.id, aliceHeaders, SC_FORBIDDEN)
        delete(res.id, adminHeaders, SC_OK)
      }

      jpost[MockSessionView]("/?doAs=bob", Map(), headers = adminHeaders) { res =>
        delete(res.id, aliceHeaders, SC_FORBIDDEN)
        delete(res.id, adminHeaders, SC_OK)
      }
    }
  }
}

class ClusterEnabledSessionServletSpec
  extends BaseSessionServletSpec[MockSession, MockRecoveryMetadata] {

  override protected def createConf(): LivyConf = {
    val livyConf = super.createConf()
    livyConf.set(LivyConf.CLUSTER_ENABLED, true)
    livyConf
  }

  override def createServlet(): SessionServlet[MockSession, MockRecoveryMetadata] = {
    SessionServletSpec.createClusterEnabledServlet(createConf())
  }

  addServlet(servlet, "/mocks/*")

  describe("SessionServlet") {
    it("should create session") {
      val headers = Map(BaseSessionServletSpec.REMOTE_USER_HEADER -> "emma")

      // allocated to this server node
      jpost[MockSessionView]("/mocks/", Map(), headers = headers) { res =>
        res.id should be(200)
        res.owner should be("emma")
        header("Location") should be("/mocks/200")
      }

      // allocated to another online server node
      post("/mocks/", toJson(Map()), headers = headers) {
        status should be(SC_TEMPORARY_REDIRECT)
        header.get("Location") should be(Some("http://127.0.0.1:8998/mocks/201"))
      }
      jpost[MockSessionView]("/mocks/201", Map(), headers = headers) { res =>
        res.id should be(201)
        res.owner should be("emma")
        header("Location") should be("/mocks/201")
      }

      // allocated to another offline server node, and then allocated to this server
      jpost[MockSessionView]("/mocks/", Map(), headers = headers) { res =>
        res.id should be(202)
        res.owner should be("emma")
        header("Location") should be("/mocks/202")
      }

      // unknown session id specified
      post("/mocks/210", toJson(Map()), headers = headers) {
        new String(bodyBytes).contains("Unknown session id 210") should be(true)
        status should be(SC_BAD_REQUEST)
      }
    }

    it("should get session") {
      jget[MockSessionView](s"/mocks/1") { res =>
        res.id should be(1)
        res.owner should be("bob")
      }
    }

    it("should recover session") {
      jget[MockSessionView](s"/mocks/100") { res =>
        res.id should be(100)
        res.owner should be("alice")
      }

      get("/mocks/101") {
        status should be(SC_NOT_FOUND)
      }

      get("/mocks/102") {
        status should be(SC_TEMPORARY_REDIRECT)
        header.get("Location") should be(Some("http://127.0.0.1:8998/mocks/102"))
      }

      jget[MockSessionView](s"/mocks/103") { res =>
        res.id should be(103)
        res.owner should be("alice")
      }
    }

    it("should get sessions in cluster") {
      jget[MockSessionViews]("/mocks") { res =>
        res.total should be(1)
        res.from should be(0)
        res.sessions.head.id should be(100)
        res.sessions.head.owner should be("alice")
      }
    }

    it("should get sessions in cluster with filter") {
      jget[MockSessionViews]("/mocks?searchKey=127.0.0.1") { res =>
        res.total should be(1)
        res.from should be(0)
        res.sessions.head.id should be(100)
        res.sessions.head.owner should be("alice")
      }
    }
  }
}

