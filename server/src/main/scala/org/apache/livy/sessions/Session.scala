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

import java.io.InputStream
import java.net.{URI, URISyntaxException}
import java.security.PrivilegedExceptionAction
import java.util.UUID

import scala.concurrent.{ExecutionContext, Future}

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.security.UserGroupInformation

import org.apache.livy.{LivyConf, Logging, ServerMetadata, Utils}
import org.apache.livy.LivyConf.{SPARK_VERSION_EDITION_PREVIEW => PREVIEW, SPARK_VERSION_EDITION_STABLE => STABLE, SPARK_VERSION_EDITION_STALE => STALE}
import org.apache.livy.utils.{AppInfo, LivySparkUtils}

object Session extends Logging {
  trait RecoveryMetadata {
    val id: Int
    val serverMetadata: ServerMetadata

    /**
     * @return if can un-allocate server-session relation in cluster mode
     */
    def isServerDeallocatable(): Boolean

    /**
     * @return if can recover the session
     */
    def isRecoverable(): Boolean
  }

  lazy val configBlackList: Set[String] = {
    val url = getClass.getResource("/spark-blacklist.conf")
    if (url != null) Utils.loadProperties(url).keySet else Set()
  }

  /**
   * Validates and prepares a user-provided configuration for submission.
   *
   * - Verifies that no blacklisted configurations are provided.
   * - Merges file lists in the configuration with the explicit lists provided in the request
   * - Resolve file URIs to make sure they reference the default FS
   * - Verify that file URIs don't reference non-whitelisted local resources
   */
  def prepareConf(
      conf: Map[String, String],
      jars: Seq[String],
      files: Seq[String],
      archives: Seq[String],
      pyFiles: Seq[String],
      livyConf: LivyConf): Map[String, String] = {
    if (conf == null) {
      return Map()
    }

    val errors = conf.keySet.filter(configBlackList.contains)
    if (errors.nonEmpty) {
      throw new IllegalArgumentException(
        "Blacklisted configuration values in session config: " + errors.mkString(", "))
    }

    val confLists: Map[String, Seq[String]] = livyConf.sparkFileLists
      .map { key => (key -> Nil) }.toMap

    val userLists = confLists ++ Map(
      LivyConf.SPARK_JARS -> jars,
      LivyConf.SPARK_FILES -> files,
      LivyConf.SPARK_ARCHIVES -> archives,
      LivyConf.SPARK_PY_FILES -> pyFiles)

    val merged = userLists.flatMap { case (key, list) =>
      val confList = conf.get(key)
        .map { list =>
          resolveURIs(list.split("[, ]+").toSeq, livyConf)
        }
        .getOrElse(Nil)
      val userList = resolveURIs(list, livyConf)
      if (confList.nonEmpty || userList.nonEmpty) {
        Some(key -> (userList ++ confList).mkString(","))
      } else {
        None
      }
    }

    val masterConfList = Map(LivyConf.SPARK_MASTER -> livyConf.sparkMaster()) ++
      livyConf.sparkDeployMode().map(LivyConf.SPARK_DEPLOY_MODE -> _).toMap

    conf ++ masterConfList ++ merged
  }

  /**
   * Prepends the value of the "fs.defaultFS" configuration to any URIs that do not have a
   * scheme. URIs are required to at least be absolute paths.
   *
   * @throws IllegalArgumentException If an invalid URI is found in the given list.
   */
  def resolveURIs(uris: Seq[String], livyConf: LivyConf): Seq[String] = {
    val defaultFS = livyConf.hadoopConf.get("fs.defaultFS").stripSuffix("/")
    uris.filter(_.nonEmpty).map { _uri =>
      val uri = try {
        new URI(_uri)
      } catch {
        case e: URISyntaxException => throw new IllegalArgumentException(e)
      }
      resolveURI(uri, livyConf).toString()
    }
  }

  def resolveURI(uri: URI, livyConf: LivyConf): URI = {
    val defaultFS = livyConf.hadoopConf.get("fs.defaultFS").stripSuffix("/")
    val resolved =
      if (uri.getScheme() == null) {
        val pathWithSegment =
          if (uri.getFragment() != null) uri.getPath() + '#' + uri.getFragment() else uri.getPath()

        require(pathWithSegment.startsWith("/"), s"Path '${uri.getPath()}' is not absolute.")
        new URI(defaultFS + pathWithSegment)
      } else {
        uri
      }

    if (resolved.getScheme() == "file") {
      // Make sure the location is whitelisted before allowing local files to be added.
      require(livyConf.localFsWhitelist.find(resolved.getPath().startsWith).isDefined,
        s"Local path ${uri.getPath()} cannot be added to user sessions.")
    }

    resolved
  }

  /**
   * Allow users to use preview or stale spark after upgrading spark. Spark versions
   * configuration should be as below:
   * livy.server.spark.versions=v3
   * livy.server.spark-home.v3=3.1
   * livy.server.spark-home.v3.preview=3.2
   * livy.server.spark-home.v3.stale=3.0
   *
   * The priority for the configuration to take effect is:
   * 1. spark.livy.spark_version_edition
   * 2. livy.server.spark_version_edition.preview.queues
   *    livy.server.spark_version_edition.stable.queues
   *    livy.server.spark_version_edition.stale.queues
   * 3. livy.server.spark.preview.queues.suffixes
   *
   * @param reqSparkVersion Spark version in user request.
   * @param queue Yarn queue.
   * @param livyConf Livy service configuration.
   * @param reqSparkVersionEdition The value should be one of stable, preview, stale.
   * @return The spark version in user request or an edited version.
   */
  def reqSparkVersionAndEdition(reqSparkVersion: Option[String],
      reqSparkVersionEdition: Option[String] = None,
      queue: Option[String], livyConf: LivyConf): Option[String] = {

    if (reqSparkVersion.isEmpty) {
      return reqSparkVersion
    }

    if (reqSparkVersionEdition.nonEmpty &&
      !Set(PREVIEW, STABLE, STALE).contains(reqSparkVersionEdition.get)) {
      throw new IllegalArgumentException(
        s"Unknown conf ${LivyConf.SPARK_VERSION_EDITION}=$reqSparkVersionEdition, " +
          s"should be one of [$STALE, $PREVIEW, $STABLE]")
    }

    val configuredSparkVersionEdition: Option[String] = if (queue.nonEmpty){
      if (livyConf.previewQueues.contains(queue.get)) Some(PREVIEW)
      else if (livyConf.stableQueues.contains(queue.get)) Some(STABLE)
      else if (livyConf.staleQueues.contains(queue.get)) Some(STALE)
      else None
    } else {
      None
    }

    val sparkVersionEdition = reqSparkVersionEdition
      .orElse(configuredSparkVersionEdition)
      .orElse(queue
        .filter(q =>
          livyConf.previewQueuesSuffixes
            .filter(StringUtils.isNotBlank(_))
            .exists(q.endsWith)
        )
        .map(_ => PREVIEW)
      )
      .getOrElse(STABLE)

    val suffix: String = sparkVersionEdition match {
      case PREVIEW | STALE => "." + sparkVersionEdition
      case _ => ""
    }
    Some(reqSparkVersion.get + suffix)
  }

  def sparkMajorFeatureVersionTuple2(reqSparkVersion: Option[String],
      livyConf: LivyConf): (Int, Int) = {

    if (reqSparkVersion.isEmpty) {
      LivySparkUtils.formatSparkVersion(livyConf.get(LivyConf.LIVY_SPARK_VERSION))
    } else {
      val confKey = LivyConf.LIVY_SPARK_VERSION.key + "." + reqSparkVersion.get
      val sparkVersion = livyConf.get(confKey)
      info(s"reqSparkVersion:$reqSparkVersion, confKey:$confKey, sparkVersion:$sparkVersion")
      LivySparkUtils.formatSparkVersion(sparkVersion)
    }
  }

  def mappingSparkAliasVersion(reqSparkAliasVersion: Option[String], queue: Option[String],
      livyConf: LivyConf): Option[String] = {
    if (reqSparkAliasVersion.isDefined
        && !livyConf.sparkAliasVersions.contains(reqSparkAliasVersion.get)) {
      throw new IllegalArgumentException("spark version is not support")
    }

    if (reqSparkAliasVersion.isDefined) {
      val queue2Version = livyConf.sparkAliasVersionMapping(reqSparkAliasVersion.get)
      queue2Version.get(queue.getOrElse("*")).orElse(queue2Version.get("*"))
    } else {
      None
    }
  }
}

abstract class Session(
    val id: Int,
    val name: Option[String],
    val owner: String,
    val livyConf: LivyConf)
  extends Logging {

  import Session._

  protected implicit val executionContext = ExecutionContext.global

  // validate session name. The name should not be a number
  name.foreach { sessionName =>
    if (sessionName.forall(_.isDigit)) {
      throw new IllegalArgumentException(s"Invalid session name: $sessionName")
    }
  }

  protected var _appId: Option[String] = None

  private var _lastActivity = System.nanoTime()

  protected val _createdTime = System.currentTimeMillis()
  protected var _startedTime: java.lang.Long = _

  // Directory where the session's staging files are created. The directory is only accessible
  // to the session's effective user.
  private var stagingDir: Path = null

  def appId: Option[String] = _appId

  var appInfo: AppInfo = AppInfo()

  def lastActivity: Long = state match {
    case SessionState.Error(time) => time
    case SessionState.Dead(time) => time
    case SessionState.Success(time) => time
    case _ => _lastActivity
  }

  def logLines(logType: Option[String] = None): IndexedSeq[String]

  def recordActivity(): Unit = {
    _lastActivity = System.nanoTime()
  }

  def recoveryMetadata: RecoveryMetadata

  def state: SessionState

  def start(): Unit

  def stop(): Future[Unit] = Future {
    try {
      info(s"Stopping $this...")
      stopSession()
      info(s"Stopped $this.")
    } catch {
      case e: Exception =>
        warn(s"Error stopping session $id.", e)
    }

    try {
      if (stagingDir != null) {
        debug(s"Deleting session $id staging directory $stagingDir")
        doAsOwner {
          val fs = FileSystem.newInstance(livyConf.hadoopConf)
          try {
            fs.delete(stagingDir, true)
          } finally {
            fs.close()
          }
        }
      }
    } catch {
      case e: Exception =>
        warn(s"Error cleaning up session $id staging dir.", e)
    }
  }


  override def toString(): String = s"${this.getClass.getSimpleName} $id"

  protected def stopSession(): Unit

  // Visible for testing.
  val proxyUser: Option[String]

  protected def doAsOwner[T](fn: => T): T = {
    val user = proxyUser.getOrElse(owner)
    if (user != null && livyConf.getBoolean(LivyConf.DO_AS_OWNER_ENABLED)) {
      val ugi = if (UserGroupInformation.isSecurityEnabled) {
        if (livyConf.getBoolean(LivyConf.IMPERSONATION_ENABLED)) {
          UserGroupInformation.createProxyUser(user, UserGroupInformation.getCurrentUser())
        } else {
          UserGroupInformation.getCurrentUser()
        }
      } else {
        UserGroupInformation.createRemoteUser(user)
      }
      ugi.doAs(new PrivilegedExceptionAction[T] {
        override def run(): T = fn
      })
    } else {
      fn
    }
  }

  protected def copyResourceToHDFS(dataStream: InputStream, name: String): URI = doAsOwner {
    val fs = FileSystem.newInstance(livyConf.hadoopConf)

    try {
      val filePath = new Path(getStagingDir(fs), name)
      debug(s"Uploading user file to $filePath")

      val outFile = fs.create(filePath, true)
      val buffer = new Array[Byte](512 * 1024)
      var read = -1
      try {
        while ({read = dataStream.read(buffer); read != -1}) {
          outFile.write(buffer, 0, read)
        }
      } finally {
        outFile.close()
      }
      filePath.toUri
    } finally {
      fs.close()
    }
  }

  private def getStagingDir(fs: FileSystem): Path = synchronized {
    if (stagingDir == null) {
      val stagingRoot = Option(livyConf.get(LivyConf.SESSION_STAGING_DIR)).getOrElse {
        new Path(fs.getHomeDirectory(), ".livy-sessions").toString()
      }

      val sessionDir = new Path(stagingRoot, UUID.randomUUID().toString())
      fs.mkdirs(sessionDir)
      fs.setPermission(sessionDir, new FsPermission("755"))
      stagingDir = sessionDir
      debug(s"Session $id staging directory is $stagingDir")
    }
    stagingDir
  }

}
