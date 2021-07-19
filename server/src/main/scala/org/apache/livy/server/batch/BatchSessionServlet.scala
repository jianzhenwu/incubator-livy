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

package org.apache.livy.server.batch

import javax.servlet.http.HttpServletRequest

import scala.util.Try

import org.apache.livy.LivyConf
import org.apache.livy.cluster.{ClusterManager, SessionAllocator}
import org.apache.livy.metrics.common.{Metrics, MetricsKey}
import org.apache.livy.server.{AccessManager, SessionServlet}
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.BatchSessionManager
import org.apache.livy.utils.AppInfo

case class BatchSessionView(
  id: Long,
  name: Option[String],
  owner: String,
  proxyUser: Option[String],
  state: String,
  appId: Option[String],
  appInfo: AppInfo,
  log: Seq[String],
  server: Option[String] = None)

class BatchSessionServlet(
    sessionManager: BatchSessionManager,
    sessionAllocator: Option[SessionAllocator],
    clusterManager: Option[ClusterManager],
    sessionStore: SessionStore,
    livyConf: LivyConf,
    accessManager: AccessManager)
  extends SessionServlet(sessionManager, sessionAllocator, clusterManager, livyConf, accessManager)
{
  override protected def createSession(
      sessionId: Int,
      req: HttpServletRequest): BatchSession = {
    val createRequest = bodyAs[CreateBatchRequest](req)

    Metrics().incrementCounter(MetricsKey.BATCH_SESSION_TOTAL_COUNT)

    BatchSession.create(
      sessionId,
      createRequest.name,
      createRequest,
      livyConf,
      accessManager,
      remoteUser(req),
      proxyUser(req, createRequest.proxyUser),
      sessionStore)
  }

  override protected[batch] def clientSessionView(
      session: BatchSession,
      req: HttpServletRequest): Any = {
    val logs =
      if (accessManager.hasViewAccess(session.owner,
                                      effectiveUser(req),
                                      session.proxyUser.getOrElse(""))) {
        val lines = session.logLines()

        val size = 10
        val from = math.max(0, lines.length - size)
        val until = from + size

        lines.view(from, until).toSeq
      } else {
        Nil
      }
    BatchSessionView(session.id, session.name, session.owner, session.proxyUser,
      session.state.toString, session.appId, session.appInfo, logs,
      Option(session.recoveryMetadata.serverMetadata.toString()))
  }

  override protected[batch] def clientSessionView(
      meta: BatchRecoveryMetadata,
      req: HttpServletRequest): Any = {

    if (accessManager.hasViewAccess(meta.owner, effectiveUser(req), meta.proxyUser.getOrElse(""))) {
      val batchSessionView: Try[BatchSessionView] = remoteSessionView[BatchSessionView](meta, req)
      if (batchSessionView.isSuccess) {
        return batchSessionView.get
      }
      SessionServlet.error(s"Error when executing request ${req.getRequestURL}\n" +
        s"BatchSessionId: ${meta.id}\n" +
        s"Error message: ${batchSessionView.failed.get.getMessage}")
    }
    BatchSessionView(meta.id,
      meta.name,
      meta.owner,
      meta.proxyUser,
      "", meta.appId, new AppInfo(), Nil,
      Option(if (meta.serverMetadata != null) { meta.serverMetadata.toString() } else { "" }))
  }

  protected def filterBySearchKey(
      recoveryMetadata: BatchRecoveryMetadata,
      searchKey: Option[String]): Boolean = {
    !searchKey.exists(_.trim.nonEmpty) ||
      filterBySearchKey(recoveryMetadata.appId, recoveryMetadata.name,
        Option(recoveryMetadata.owner), recoveryMetadata.proxyUser,
        recoveryMetadata.serverMetadata, searchKey.get)
  }

  protected def filterBySearchKey(
      session: BatchSession,
      searchKey: Option[String]): Boolean = {
    !searchKey.exists(_.trim.nonEmpty) ||
      filterBySearchKey(session.appId, session.name,
        Option(session.owner), session.proxyUser,
        session.recoveryMetadata.serverMetadata, searchKey.get)
  }

}
