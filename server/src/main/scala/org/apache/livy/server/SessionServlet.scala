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

import java.net.URI
import java.security.AccessControlException
import javax.servlet.http.HttpServletRequest

import scala.concurrent._
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.reflect.classTag
import scala.util.{Failure, Success, Try}

import com.fasterxml.jackson.databind.ObjectMapper
import com.squareup.okhttp.{Headers, HttpUrl, OkHttpClient, Request}
import org.scalatra._
import org.slf4j.MDC

import org.apache.livy.{LivyConf, Logging, ServerMetadata}
import org.apache.livy.cluster.{ClusterManager, ServerNode, SessionAllocator}
import org.apache.livy.metrics.common.{Metrics, MetricsKey}
import org.apache.livy.rsc.RSCClientFactory
import org.apache.livy.server.batch.BatchSession
import org.apache.livy.sessions.{Session, SessionManager}
import org.apache.livy.sessions.Session.RecoveryMetadata
import org.apache.livy.utils.LivyProcessorException

object SessionServlet extends Logging

/**
 * Base servlet for session management. All helper methods in this class assume that the session
 * id parameter in the handler's URI is "id".
 *
 * Type parameters:
 *  S: the session type
 */
abstract class SessionServlet[S <: Session, R <: RecoveryMetadata](
    private[livy] val sessionManager: SessionManager[S, R],
    val sessionAllocator: Option[SessionAllocator],
    val clusterManager: Option[ClusterManager],
    val livyConf: LivyConf,
    accessManager: AccessManager,
    mockHttpClient: Option[OkHttpClient] = None)(implicit c: ClassTag[R])
  extends JsonServlet
  with ApiVersioningSupport
  with MethodOverride
  with UrlGeneratorSupport
  with ContentEncodingSupport
{
  val httpClient: OkHttpClient = mockHttpClient.getOrElse(new OkHttpClient())
  val objectMapper: ObjectMapper = new ObjectMapper()
    .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)



  /**
   * Creates a new session based on the current request. The implementation is responsible for
   * parsing the body of the request.
   */
  protected def createSession(sessionId: Int, req: HttpServletRequest): S

  /**
   * Returns a object representing the session data to be sent back to the client.
   */
  protected def clientSessionView(session: S, req: HttpServletRequest): Any = session

  protected def clientSessionView(recoverMetadata: R, req: HttpServletRequest): Any = session

  protected def clientSessionCreationView(session: S, req: HttpServletRequest): Any = session

  /**
   * Return true when one of id, appId, name, owner, proxyUser, Metadata matches searchKey.
   */
  protected def filterBySearchKey(recoveryMetadata: R, searchKey: Option[String]): Boolean

  /**
   * Return true when one of id, appId, name, owner, proxyUser, Metadata matches searchKey.
   */
  protected def filterBySearchKey(session: S, searchKey: Option[String]): Boolean

  /**
   * Get session owner by SessionStore when the service where the session is hung up.
   * Return null when session is not exist.
   */
  protected def getSessionOwnerFromSessionStore(sessionId: Int): String

  override def shutdown(): Unit = {
    sessionManager.shutdown()
  }

  before() {
    contentType = "application/json"
  }

  get("/") {
    val from = params.get("from").map(_.toInt).getOrElse(0)
    val size = params.get("size").map(_.toInt).getOrElse(100)
    val searchKey = params.get("searchKey")
    Metrics().startStoredScope(MetricsKey.REST_SESSION_LIST_PROCESSING_TIME)
    if(livyConf.getBoolean(LivyConf.CLUSTER_ENABLED)) {

      val sessions = sessionAllocator.map(e => {
        e.getAllSessions[R](sessionManager.sessionType(), None)
      }).orElse(Some(Seq(Failure(null))))
        .get
        .filter(e => e.isSuccess && filterBySearchKey(e.get, searchKey))
        .sortWith(_.get.id > _.get.id)

      Metrics().endStoredScope(MetricsKey.REST_SESSION_LIST_PROCESSING_TIME)
      Map(
        "from" -> from,
        "total" -> sessions.size,
        "sessions" -> sessions.view(from, from + size)
          .map(e => clientSessionView(e.get, request))
      )
    } else {
      val sessions = sessionManager.all().filter(filterBySearchKey(_, searchKey))
        .toSeq
        .sortWith(_.id > _.id)
      Metrics().endStoredScope(MetricsKey.REST_SESSION_LIST_PROCESSING_TIME)
      Map(
        "from" -> from,
        "total" -> sessions.size,
        "sessions" -> sessions.view(from, from + size)
          .map(e => clientSessionView(e, request))
      )
    }

  }

  val getSession = get("/:id") {
    withUnprotectedSession { session =>
      clientSessionView(session, request)
    }
  }

  get("/:id/state") {
    withUnprotectedSession { session =>
      Map("id" -> session.id, "state" -> session.state.toString)
    }
  }

  get("/:id/log") {
    withViewAccessSession { session =>
      val from = params.get("from").map(_.toInt)
      val size = params.get("size").map(_.toInt)
      val logType = params.get("logType")
      val (from_, total, logLines) = serializeLogs(session, from, size, logType)

      if (request.getHeader("Accept") == "text/plain") {
        logLines.mkString("\n")
      } else {
        Map(
          "id" -> session.id,
          "from" -> from_,
          "total" -> total,
          "log" -> logLines)
      }
    }
  }

  get("/:id/amlogs/:logType") {
    withViewAccessSession { session =>
      val from = params.get("from").map(_.toInt)
      val size = params.get("size").map(_.toInt)
      val logType = params("logType")
      session.appId match {
        case Some(appId) => yarnApplicationLog(appId, logType, from, size)
        case _ => BadRequest(s"ApplicationId of session ${session.id} not found.")
      }
    }
  }

  delete("/:id") {
    withModifyAccessSession { session =>
      sessionManager.delete(session.id) match {
        case Some(future) =>
          Await.ready(future, Duration.Inf)
          Ok(ResponseMessage("deleted"))

        case None =>
          NotFound(ResponseMessage(s"Session ${session.id} already stopped."))
      }
    }
  }

  def tooManySessions(): Boolean = {
    val totalChildProceses = RSCClientFactory.childProcesses().get() +
      BatchSession.childProcesses.get()
    totalChildProceses >= livyConf.getInt(LivyConf.SESSION_MAX_CREATION)
  }

  /**
   * Deallocating server when the session can be deallocated, otherwise catch the
   * IllegalStateException and do nothing.
   * @param sessionId the id of session.
   */
  def deallocateServerQuietly(sessionId: Int): Unit = {
    sessionAllocator.foreach { allocator =>
      try {
        allocator.deallocateServer(sessionManager.sessionType(), sessionId)
      } catch {
        case ise: IllegalStateException =>
          SessionServlet.error(s"Fail to deallocate server for " +
            s"${sessionManager.sessionType()} $sessionId", ise)
      }
    }
  }

  private def createSessionInternal(sessionId: Int): ActionResult = {
    val serverNode = if (sessionAllocator.isDefined) {
      sessionAllocator.flatMap(_.findServer[R](sessionManager.sessionType(), sessionId))
    } else {
      Some(ServerNode(livyConf.serverMetadata()))
    }

    if(serverNode.isEmpty) {
      // in cluster mode, but session allocator does not know the id
      BadRequest(ResponseMessage(s"Unknown session id $sessionId"))
    } else if (serverNode.get.serverMetadata == livyConf.serverMetadata()) {
      // session allocated to current server or in standalone mode
      synchronized {
        if (tooManySessions) {
          deallocateServerQuietly(sessionId)
          BadRequest(ResponseMessage("Rejected, too many sessions are being created!"))
        } else {
          MDC.clear()
          MDC.put("session",
            s"${sessionManager.sessionType()}Session-${sessionId.toString}")
          Metrics().startStoredScope(MetricsKey.REST_SESSION_CREATE_PROCESSING_TIME)
          val session = try {
            sessionManager.register(createSession(sessionId, request))
          } catch {
            case iae: IllegalArgumentException =>
              deallocateServerQuietly(sessionId)
              throw iae
            case pe: LivyProcessorException =>
              deallocateServerQuietly(sessionId)
              throw pe
          }
          Metrics().endStoredScope(MetricsKey.REST_SESSION_CREATE_PROCESSING_TIME)
          // Because it may take some time to establish the session, update the last activity
          // time before returning the session info to the client.
          session.recordActivity()
          Created(clientSessionCreationView(session, request),
            headers = Map("Location" ->
              url(getSession, "id" -> session.id.toString)))
        }
      }
    } else if (clusterManager.map(_.isNodeOnline(serverNode.get)).orElse(Some(false)).get) {
      // session allocated to another online server
      val uri = new URI(url(createSpecificSession, "id" -> sessionId.toString))
      TemporaryRedirect(new URI(request.getScheme, null,
        serverNode.get.host, serverNode.get.port,
        uri.getPath, uri.getQuery, null).toString)
    } else {
      // session allocated to another offline server
      sessionAllocator.foreach(_.allocateServer[R](sessionManager.sessionType(),
        sessionId, remoteUser(request)))
      createSessionInternal(sessionId)
    }
  }

  post("/") {
    val sessionId = sessionManager.nextId()
    sessionAllocator.foreach(_.allocateServer[R](sessionManager.sessionType(), sessionId,
      remoteUser(request)))
    createSessionInternal(sessionId)
  }

  val createSpecificSession = post("/:id") {
    val sessionId = params("id").toInt
    createSessionInternal(sessionId)
  }

  private def getRequestPathInfo(request: HttpServletRequest): String = {
    if (request.getPathInfo != null && request.getPathInfo != "/") {
      request.getPathInfo
    } else {
      ""
    }
  }

  error {
    case e: IllegalArgumentException => BadRequest(ResponseMessage(e.getMessage))
    case e: AccessControlException => Forbidden(ResponseMessage(e.getMessage))
  }

  /**
   * Returns the remote user for the given request. Separate method so that tests can override it.
   */
  protected def remoteUser(req: HttpServletRequest): String = req.getRemoteUser()

  /**
   * Returns the impersonated user as given by "doAs" as a request parameter.
   */
  protected def impersonatedUser(request: HttpServletRequest): Option[String] = {
    Option(request.getParameter("doAs"))
  }

  /**
   * Returns the proxyUser for the given request.
   */
  protected def proxyUser(
      request: HttpServletRequest,
      createRequestProxyUser: Option[String]): Option[String] = {
    impersonatedUser(request).orElse(createRequestProxyUser)
  }

  /**
   * Gets the request user or impersonated user to determine the effective user.
   */
  protected def effectiveUser(request: HttpServletRequest): String = {
    val requestUser = remoteUser(request)
    accessManager.checkImpersonation(impersonatedUser(request), requestUser).getOrElse(requestUser)
  }

  /**
   * Performs an operation on the session, without checking for ownership. Operations executed
   * via this method must not modify the session in any way, or return potentially sensitive
   * information.
   */
  protected def withUnprotectedSession(fn: (S => Any)): Any = doWithSession(fn, true, None)

  /**
   * Performs an operation on the session, verifying whether the caller has view access of the
   * session.
   */
  protected def withViewAccessSession(fn: (S => Any)): Any =
    doWithSession(fn, false, Some(accessManager.hasViewAccess))

  /**
   * Performs an operation on the session, verifying whether the caller has view access of the
   * session.
   */
  protected def withModifyAccessSession(fn: (S => Any)): Any =
    doWithSession(fn, false, Some(accessManager.hasModifyAccess))

  private def doWithSession(fn: (S => Any),
      allowAll: Boolean,
      checkFn: Option[(String, String, String) => Boolean]): Any = {
    val idOrNameParam: String = params("id")
    val (sessionId: Option[Int], session) = if (idOrNameParam.forall(_.isDigit)) {
      val sessionId = idOrNameParam.toInt
      (Some(sessionId), sessionManager.get(sessionId))
    } else {
      val sessionName = idOrNameParam
      (None, sessionManager.get(sessionName))
    }

    MDC.clear()
    MDC.put("session", s"${sessionManager.sessionType()}Session-${sessionId.toString}")
    session match {
      case Some(session) =>
        Metrics().incrementCounter(MetricsKey.REST_SESSION_FOUND_IN_MANAGER_COUNT)
        if (allowAll ||
            checkFn.map(_(session.owner,
                          effectiveUser(request),
                          session.proxyUser.getOrElse("")))
                   .getOrElse(false)) {
          fn(session)
        } else {
          Forbidden()
        }
      case None =>
        val serverNode = sessionId.flatMap(sid => {
          sessionAllocator
            .flatMap(_.findServer(sessionManager.sessionType(), sid))
            .orElse(None)
        }).orElse(None)

        if(serverNode.isEmpty) {
          Metrics().incrementCounter(MetricsKey.REST_SESSION_FOUND_IN_NOWHERE_COUNT)
          // session id is None, session allocator is None or cannot recognize session id
          NotFound(ResponseMessage(s"Session '$idOrNameParam' not found."))
        } else {
          Metrics().incrementCounter(MetricsKey.REST_SESSION_FOUND_IN_ALLOCATOR_COUNT)
          if (serverNode.get.serverMetadata == livyConf.serverMetadata()) {
            sessionManager.recover(sessionId.get)
            doWithSession(fn, allowAll, checkFn)
          } else if (clusterManager.map(_.isNodeOnline(serverNode.get)).orElse(Some(false)).get) {
            TemporaryRedirect(new URI(request.getScheme, null,
              serverNode.get.host, serverNode.get.port,
              request.getRequestURI, request.getQueryString, null).toString)
          } else {
            try {
              sessionAllocator.foreach({
                _.allocateServer[R](sessionManager.sessionType(), sessionId.get,
                  getSessionOwnerFromSessionStore(sessionId.get))
              })
              doWithSession(fn, allowAll, checkFn)
            } catch {
              case e: Exception => NotFound(ResponseMessage(s"${e.getMessage}"))
            }
          }
        }
    }
  }

  private def serializeLogs(session: S, fromOpt: Option[Int], sizeOpt: Option[Int],
       logType: Option[String] = None) = {
    val lines = session.logLines(logType)

    var size = sizeOpt.getOrElse(100)
    var from = fromOpt.getOrElse(-1)
    if (size < 0) {
      size = lines.length
    }
    if (from < 0) {
      from = math.max(0, lines.length - size)
    }
    val until = from + size

    (from, lines.length, lines.view(from, until))
  }


  protected def remoteSessionView[T: ClassTag](
      recoveryMetadata: R,
      req: HttpServletRequest) : Try[T] = {
    try {
      val serverNode: Option[ServerNode] = sessionAllocator.get
        .findServer(sessionManager.sessionType(), recoveryMetadata.id)
      val host = serverNode.get.host
      val port = serverNode.get.port
      val path = url(getSession, "id" -> recoveryMetadata.id.toString)
      val scheme = req.getScheme
      val requestUrl = s"$scheme://$host:$port$path"
      val res = httpClient.newCall(new Request.Builder().url(requestUrl).build()).execute()
      val body = res.body().string()
      res.body().close()
      Success(objectMapper.readValue(body, classTag[T].runtimeClass).asInstanceOf[T])
    } catch {
      case exception: Throwable => Failure(exception)
    }
  }


  /**
   * Return true when appId, name or serverMetadata match searchKey which is not empty.
   */
  protected def filterBySearchKey(
      id: Int,
      appId: Option[String],
      name: Option[String],
      owner: Option[String],
      proxyUser: Option[String],
      serverMetadata: ServerMetadata,
      searchKey: String): Boolean = {

    def matchSearchKey(value: Option[String], searchKey: String): Boolean = {
      value.exists(_.trim.nonEmpty) && value.get.trim.contains(searchKey.trim)
    }
    matchSearchKey(Some(id.toString), searchKey) ||
      matchSearchKey(appId, searchKey) || matchSearchKey(name, searchKey) ||
      matchSearchKey(owner, searchKey) || matchSearchKey(proxyUser, searchKey) ||
      (serverMetadata != null && matchSearchKey(Option(serverMetadata.toString()), searchKey))
  }

  def yarnApplicationLog(appId: String, logType: String,
      fromOpt: Option[Int], sizeOpt: Option[Int]): Any = {

    val from = fromOpt.getOrElse(0)

    var appLog = s"Application $appId not found"

    try {
      val urlAmLog = URI.create(livyConf.get(LivyConf.YARN_TIMELINE_SERVER)).resolve(
        s"/ws/v2/applicationlog/apps/$appId/amlogs/$logType").toString

      val httpBuilder = HttpUrl.parse(urlAmLog)
        .newBuilder()
        .addQueryParameter("start", from.toString)

      sizeOpt.foreach { size =>
        httpBuilder.addQueryParameter("size", size.toString)
      }

      TemporaryRedirect.apply(httpBuilder.build().toString)
    } catch {
      case exception: Exception =>
        appLog = s"Fail to get application $appId log." +
          s"ErrorMessage: ${exception.getMessage}"
        InternalServerError.apply(appLog)
    }
  }

  def doRemoteGetRaw(url: HttpUrl, headers: java.util.Map[String, String]): String = {
    val requestBuilder = new Request.Builder()
    if (headers != null && !headers.isEmpty) {
      requestBuilder.headers(Headers.of(headers))
    }
    val request = requestBuilder.url(url).build()
    val res = httpClient.newCall(request).execute()
    val body = res.body().string()
    res.body().close()
    body
  }

  def doRemoteGet[T: ClassTag](url: HttpUrl, headers: java.util.Map[String, String]): T = {
    val body = this.doRemoteGetRaw(url, headers)
    objectMapper.readValue(body, classTag[T].runtimeClass).asInstanceOf[T]
  }
}
