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

package org.apache.livy.server.interactive

import java.net.URI
import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Try

import org.json4s.jackson.Json4sScalaModule
import org.scalatra._
import org.scalatra.servlet.FileUploadSupport

import org.apache.livy.{CompletionRequest, ExecuteRequest, JobHandle, LivyConf, Logging}
import org.apache.livy.client.common.HttpMessages
import org.apache.livy.client.common.HttpMessages._
import org.apache.livy.cluster.{ClusterManager, SessionAllocator}
import org.apache.livy.metrics.common.{Metrics, MetricsKey}
import org.apache.livy.rsc.driver.StatementState
import org.apache.livy.server.{AccessManager, SessionServlet}
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions._
import org.apache.livy.utils.AppInfo

object InteractiveSessionServlet extends Logging

class InteractiveSessionServlet(
    sessionManager: InteractiveSessionManager,
    sessionAllocator: Option[SessionAllocator],
    clusterManager: Option[ClusterManager],
    sessionStore: SessionStore,
    livyConf: LivyConf,
    accessManager: AccessManager)
  extends SessionServlet(sessionManager, sessionAllocator, clusterManager, livyConf, accessManager)
  with SessionHeartbeatNotifier[InteractiveSession, InteractiveRecoveryMetadata]
  with FileUploadSupport
{

  mapper.registerModule(new SessionKindModule())
    .registerModule(new Json4sScalaModule())

  override protected def createSession(
      sessionId: Int,
      req: HttpServletRequest): InteractiveSession = {
    val createRequest = bodyAs[CreateInteractiveRequest](req)

    Metrics().incrementCounter(MetricsKey.INTERACTIVE_SESSION_TOTAL_COUNT)

    InteractiveSession.create(
      sessionId,
      createRequest.name,
      remoteUser(req),
      proxyUser(req, createRequest.proxyUser),
      livyConf,
      accessManager,
      createRequest,
      sessionStore)
  }

  override protected[interactive] def clientSessionView(
      session: InteractiveSession,
      req: HttpServletRequest): Any = {
    val logs = sessionLogs(session, req)

    new SessionInfo(session.id, session.name.orNull, session.appId.orNull, session.owner,
      session.proxyUser.orNull, session.state.toString,
      if (session.kind != null){ session.kind.toString } else { "" },
      session.appInfo.asJavaMap, logs.asJava, session.recoveryMetadata.serverMetadata.toString())
  }

  private def sessionLogs(session: InteractiveSession, req: HttpServletRequest): Seq[String] = {
    val logs =
      if (accessManager.hasViewAccess(session.owner,
                                      effectiveUser(req),
                                      session.proxyUser.getOrElse(""))) {
        Option(session.logLines())
          .map { lines =>
            val size = 10
            val from = math.max(0, lines.length - size)
            val until = from + size

            lines.view(from, until)
          }
          .getOrElse(Nil)
      } else {
        Nil
      }
    logs
  }

  override protected[interactive] def clientSessionView(
      meta: InteractiveRecoveryMetadata,
      req: HttpServletRequest): Any = {
    if (accessManager.hasViewAccess(meta.owner,
      effectiveUser(req),
      meta.proxyUser.getOrElse(""))) {

      val sessionView: Try[SessionInfo] = remoteSessionView[SessionInfo](meta, req)
      if (sessionView.isSuccess) {
        return sessionView.get
      }
      InteractiveSessionServlet.error(s"Error when executing request: ${req.getRequestURL}\n" +
        s"SessionId: ${meta.id}\n" +
        s"Error message: ${sessionView.failed.get.getMessage}")
    }
    new SessionInfo(meta.id,
      meta.name.orNull,
      meta.appId.orNull,
      meta.owner,
      meta.proxyUser.orNull,
      "",
      if (meta.kind != null) { meta.kind.toString } else { "" },
      new AppInfo().asJavaMap,
      new java.util.ArrayList[String](),
      if (meta.serverMetadata != null) { meta.serverMetadata.toString()} else { "" })
  }

  override protected[interactive] def clientSessionCreationView(session: InteractiveSession,
      req: HttpServletRequest): Any = {

    val logs = sessionLogs(session, req)
    new SessionCreationInfo(session.id, session.name.orNull, session.appId.orNull, session.owner,
      session.proxyUser.orNull, session.state.toString,
      if (session.kind != null){ session.kind.toString } else { "" },
      session.appInfo.asJavaMap, logs.asJava, session.recoveryMetadata.serverMetadata.toString(),
      session.optimizedConf.get.toMap.asJava)
  }

  post("/:id/stop") {
    withModifyAccessSession { session =>
      Await.ready(session.stop(), Duration.Inf)
      NoContent()
    }
  }

  post("/:id/interrupt") {
    withModifyAccessSession { session =>
      Await.ready(session.interrupt(), Duration.Inf)
      Ok(Map("msg" -> "interrupted"))
    }
  }

  get("/:id/statements") {
    withViewAccessSession { session =>
      val statements = session.statements
      val from = params.get("from").map(_.toInt).getOrElse(0)
      val size = params.get("size").map(_.toInt).getOrElse(statements.length)

      Map(
        "total_statements" -> statements.length,
        "statements" -> statements.view(from, from + size)
      )
    }
  }

  val getStatement = get("/:id/statements/:statementId") {
    withViewAccessSession { session =>
      val statementId = params("statementId").toInt
      val statement = session.getStatement(statementId)

      // add metric when get statement, because livy doesn't know statement state until client fetch
      statement.map { s =>
        if (s.state.get().isOneOf(StatementState.Available)) {
          Metrics().incrementCounter(MetricsKey.INTERACTIVE_SESSION_STATEMENT_SUCCEED_COUNT)
          Metrics().updateTimer(MetricsKey.INTERACTIVE_SESSION_STATEMENT_PROCESSING_TIME,
            (s.completed - s.started), TimeUnit.MILLISECONDS)
        }
      }

      statement.getOrElse(NotFound("Statement not found"))
    }
  }

  jpost[ExecuteRequest]("/:id/statements") { req =>
    withModifyAccessSession { session =>
      val statement = session.executeStatement(req)

      Metrics().incrementCounter(MetricsKey.INTERACTIVE_SESSION_STATEMENT_TOTAL_COUNT)

      Created(statement,
        headers = Map(
          "Location" -> url(getStatement,
            "id" -> session.id.toString,
            "statementId" -> statement.id.toString)))
    }
  }

  jpost[CompletionRequest]("/:id/completion") { req =>
    withModifyAccessSession { session =>
      val compl = session.completion(req)
      Ok(Map("candidates" -> compl.candidates))
    }
  }

  post("/:id/statements/:statementId/cancel") {
    withModifyAccessSession { session =>
      val statementId = params("statementId")
      session.cancelStatement(statementId.toInt)
      Ok(Map("msg" -> "canceled"))
    }
  }
  // This endpoint is used by the client-http module to "connect" to an existing session and
  // update its last activity time. It performs authorization checks to make sure the caller
  // has access to the session, so even though it returns the same data, it behaves differently
  // from get("/:id").
  post("/:id/connect") {
    withModifyAccessSession { session =>
      session.recordActivity()
      Ok(clientSessionView(session, request))
    }
  }

  jpost[SerializedJob]("/:id/submit-job") { req =>
    withModifyAccessSession { session =>
      try {
      require(req.job != null && req.job.length > 0, "no job provided.")
      val jobId = session.submitJob(req.job, req.jobType)
      Created(new JobStatus(jobId, JobHandle.State.SENT, null, null))
      } catch {
        case e: Throwable =>
        throw e
      }
    }
  }

  jpost[SerializedJob]("/:id/run-job") { req =>
    withModifyAccessSession { session =>
      require(req.job != null && req.job.length > 0, "no job provided.")
      val jobId = session.runJob(req.job, req.jobType)
      Created(new JobStatus(jobId, JobHandle.State.SENT, null, null))
    }
  }

  post("/:id/upload-jar") {
    withModifyAccessSession { lsession =>
      fileParams.get("jar") match {
        case Some(file) =>
          lsession.addJar(file.getInputStream, file.name)
        case None =>
          BadRequest("No jar sent!")
      }
    }
  }

  post("/:id/upload-pyfile") {
    withModifyAccessSession { lsession =>
      fileParams.get("file") match {
        case Some(file) =>
          lsession.addJar(file.getInputStream, file.name)
        case None =>
          BadRequest("No file sent!")
      }
    }
  }

  post("/:id/upload-file") {
    withModifyAccessSession { lsession =>
      fileParams.get("file") match {
        case Some(file) =>
          lsession.addFile(file.getInputStream, file.name)
        case None =>
          BadRequest("No file sent!")
      }
    }
  }

  jpost[AddResource]("/:id/add-jar") { req =>
    withModifyAccessSession { lsession =>
      addJarOrPyFile(req, lsession)
    }
  }

  jpost[AddResource]("/:id/add-pyfile") { req =>
    withModifyAccessSession { lsession =>
      addJarOrPyFile(req, lsession)
    }
  }

  jpost[AddResource]("/:id/add-file") { req =>
    withModifyAccessSession { lsession =>
      val uri = new URI(req.uri)
      lsession.addFile(uri)
    }
  }

  get("/:id/jobs/:jobid") {
    withViewAccessSession { lsession =>
      val jobId = params("jobid").toLong
      Ok(lsession.jobStatus(jobId))
    }
  }

  post("/:id/jobs/:jobid/cancel") {
    withModifyAccessSession { lsession =>
      val jobId = params("jobid").toLong
      lsession.cancelJob(jobId)
    }
  }

  private def addJarOrPyFile(req: HttpMessages.AddResource, session: InteractiveSession): Unit = {
    val uri = new URI(req.uri)
    session.addJar(uri)
  }

  protected def filterBySearchKey(
      recoveryMetadata: InteractiveRecoveryMetadata,
      searchKey: Option[String]): Boolean = {
    !searchKey.exists(_.trim.nonEmpty) ||
      filterBySearchKey(recoveryMetadata.id,
        recoveryMetadata.appId, recoveryMetadata.name,
        Option(recoveryMetadata.owner), recoveryMetadata.proxyUser,
        recoveryMetadata.serverMetadata, searchKey.get)
  }

  protected def filterBySearchKey(
      session: InteractiveSession,
      searchKey: Option[String]): Boolean = {
    !searchKey.exists(_.trim.nonEmpty) ||
      filterBySearchKey(session.id, session.appId, session.name,
        Option(session.owner), session.proxyUser,
        session.recoveryMetadata.serverMetadata, searchKey.get)
  }

  protected def getSessionOwnerFromSessionStore(sessionId: Int) : String = {
    val recoveryMetadata = sessionStore.get[InteractiveRecoveryMetadata](
      sessionManager.sessionType(), sessionId)
    if (recoveryMetadata.isDefined) {
      return recoveryMetadata.get.owner
    }
    throw new IllegalStateException(s"InteractiveRecoveryMetadata of session $sessionId not found.")
  }
}
