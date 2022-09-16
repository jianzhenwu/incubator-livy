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

import java.io.{BufferedInputStream, InputStream}
import java.net.InetAddress
import java.util.EnumSet
import java.util.concurrent._
import javax.servlet._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.apache.hadoop.security.{SecurityUtil, UserGroupInformation}
import org.apache.hadoop.security.authentication.server._
import org.eclipse.jetty.servlet.FilterHolder
import org.scalatra.{NotFound, ScalatraServlet}
import org.scalatra.metrics.MetricsBootstrap
import org.scalatra.metrics.MetricsSupportExtensions._
import org.scalatra.servlet.{MultipartConfig, ServletApiImplicits}

import org.apache.livy._
import org.apache.livy.cluster.{ClusterManager, ServerAllocator, SessionAllocator, ZookeeperClusterManager}
import org.apache.livy.metrics.common.Metrics
import org.apache.livy.server.auth.LdapAuthenticationHandlerImpl
import org.apache.livy.server.batch.BatchSessionServlet
import org.apache.livy.server.event.Events
import org.apache.livy.server.hdfs.HdfsServlet
import org.apache.livy.server.interactive.InteractiveSessionServlet
import org.apache.livy.server.recovery.{SessionStore, StateStore, ZooKeeperManager}
import org.apache.livy.server.ui.{JmxJsonServlet, UIServlet}
import org.apache.livy.sessions.{BatchSessionManager, InteractiveSessionManager, SessionIdGenerator}
import org.apache.livy.sessions.SessionManager.SESSION_RECOVERY_MODE_OFF
import org.apache.livy.utils.LivySparkUtils._
import org.apache.livy.utils.SparkYarnApp

class LivyServer extends Logging {

  import LivyConf._

  private var server: WebServer = _
  private var _serverUrl: Option[String] = None
  // make livyConf accessible for testing
  private[livy] var livyConf: LivyConf = _

  private var kinitFailCount: Int = 0
  private var executor: ScheduledExecutorService = _
  private var accessManager: AccessManager = _
  private var _thriftServerFactory: Option[ThriftServerFactory] = None

  private var zkManager: Option[ZooKeeperManager] = None
  private var clusterManager: Option[ClusterManager] = None
  private var sessionAllocator: Option[SessionAllocator] = None
  private var serverAllocator: Option[ServerAllocator] = None

  private var ugi: UserGroupInformation = _

  private var sessionStagingDirManager: SessionStagingDirManager = _

  def start(): Unit = {
    livyConf = new LivyConf().loadFromFile("livy.conf")
    accessManager = new AccessManager(livyConf)

    val host = livyConf.get(SERVER_HOST)
    val port = livyConf.getInt(SERVER_PORT)
    val basePath = livyConf.get(SERVER_BASE_PATH)
    val multipartConfig = MultipartConfig(
        maxFileSize = Some(livyConf.getLong(LivyConf.FILE_UPLOAD_MAX_SIZE))
      ).toMultipartConfigElement

    // get spark an scalar version from execute "spark-submit --version"
    def setSparkScalaVersion(versionConfKey: Option[String] = None): Unit = {
      // Make sure the `spark-submit` program exists, otherwise much of livy won't work.
      testSparkHome(livyConf, versionConfKey)

      // Test spark-submit and get Spark Scala version accordingly.
      val (sparkVersion, scalaVersionFromSparkSubmit) = sparkSubmitVersion(livyConf, versionConfKey)
      testSparkVersion(sparkVersion)

      val formattedSparkVersion = formatSparkVersion(sparkVersion)
      if (versionConfKey.isEmpty) {
        // If Spark and Scala version is set manually, should verify if they're consistent with
        // ones parsed from "spark-submit --version"
        Option(livyConf.get(LIVY_SPARK_VERSION)).map(formatSparkVersion).foreach { version =>
          require(formattedSparkVersion == version,
            s"Configured Spark version $version is not equal to Spark version " +
              s"$formattedSparkVersion " + "got from spark-submit -version")
        }

        // Set formatted Spark and Scala version into livy configuration, this will be used by
        // session creation.
        // TODO Create a new class to pass variables from LivyServer to sessions and remove these
        // internal LivyConfs.
        livyConf.set(LIVY_SPARK_VERSION.key, formattedSparkVersion.productIterator.mkString("."))
        livyConf.set(LIVY_SPARK_SCALA_VERSION.key,
          sparkScalaVersion(formattedSparkVersion, scalaVersionFromSparkSubmit, livyConf))
      } else {
        Option(livyConf.get(LIVY_SPARK_VERSION.key + "." + versionConfKey.get))
          .map(formatSparkVersion).foreach { version =>
          require(formattedSparkVersion == version,
            s"Configured Spark version $version is not equal to Spark version " +
              s"$formattedSparkVersion got from spark-submit -version")
        }

        livyConf.set(LIVY_SPARK_VERSION.key + "." + versionConfKey.get,
          formattedSparkVersion.productIterator.mkString("."))
        livyConf.set(LIVY_SPARK_SCALA_VERSION.key + "." + versionConfKey.get,
          sparkScalaVersion(formattedSparkVersion, scalaVersionFromSparkSubmit,
            livyConf, versionConfKey))
      }
    }

    setSparkScalaVersion()
    if (livyConf.sparkVersions.nonEmpty) {
      livyConf.sparkVersions.foreach(v => {
        setSparkScalaVersion(Some(v))
      })
    }

    if (livyConf.getBoolean(LivyConf.THRIFT_SERVER_ENABLED)) {
      _thriftServerFactory = Some(ThriftServerFactory.getInstance)
    }

    if (UserGroupInformation.isSecurityEnabled) {
      // If Hadoop security is enabled, run kinit periodically. runKinit() should be called
      // before any Hadoop operation, otherwise Kerberos exception will be thrown.
      executor = Executors.newScheduledThreadPool(1,
        new ThreadFactory() {
          override def newThread(r: Runnable): Thread = {
            val thread = new Thread(r)
            thread.setName("kinit-thread")
            thread.setDaemon(true)
            thread
          }
        }
      )
      val launch_keytab = livyConf.get(LAUNCH_KERBEROS_KEYTAB)
      val launch_principal = SecurityUtil.getServerPrincipal(
        livyConf.get(LAUNCH_KERBEROS_PRINCIPAL), host)
      require(launch_keytab != null,
        s"Kerberos requires ${LAUNCH_KERBEROS_KEYTAB.key} to be provided.")
      require(launch_principal != null,
        s"Kerberos requires ${LAUNCH_KERBEROS_PRINCIPAL.key} to be provided.")
      if (!runKinit(launch_keytab, launch_principal)) {
        error("Failed to run kinit, stopping the server.")
        sys.exit(1)
      }
      // This is and should be the only place where a login() on the UGI is performed.
      // If an other login in the codebase is strictly needed, a needLogin check should be added to
      // avoid anyway that 2 logins are performed.
      // This is needed because the thriftserver requires the UGI to be created from a keytab in
      // order to work properly and previously Livy was using a UGI generated from the cached TGT
      // (created by the kinit command).
      if (livyConf.getBoolean(LivyConf.THRIFT_SERVER_ENABLED)) {
        UserGroupInformation.loginUserFromKeytab(launch_principal, launch_keytab)
      }
      ugi = UserGroupInformation.getCurrentUser
      startKinitThread(launch_keytab, launch_principal)
    }

    testRecovery(livyConf)

    if (livyConf.getBoolean(FS_S3A_ENABLED)) {
      livyConf.loadS3aConfFromFile("livy-s3a.conf")
    }

    if (livyConf.getBoolean(SESSION_STAGING_DIR_CLEAN_ENABLED)) {
      sessionStagingDirManager = new SessionStagingDirManager(livyConf)
      sessionStagingDirManager.start()
    }

    // Initialize YarnClient ASAP to save time.
    if (livyConf.isRunningOnYarn()) {
      SparkYarnApp.init(livyConf)
      Future { SparkYarnApp.masterYarnIdClientMap }
    }

    if (livyConf.get(LivyConf.RECOVERY_STATE_STORE) == "zookeeper" ||
        livyConf.getBoolean(LivyConf.CLUSTER_ENABLED)) {
      zkManager = Some(new ZooKeeperManager(livyConf))
      zkManager.foreach(_.start())
    }

    StateStore.init(livyConf, zkManager)
    Metrics.init(livyConf)
    Events.init(livyConf)

    val sessionStore = new SessionStore(livyConf)

    if (livyConf.getBoolean(LivyConf.CLUSTER_ENABLED)) {
      clusterManager = Some(new ZookeeperClusterManager(livyConf, zkManager.get))
      serverAllocator = Some(new ServerAllocator(livyConf, clusterManager.get))
      sessionAllocator = Some(SessionAllocator(livyConf, clusterManager.get, sessionStore,
        zkManager.get, serverAllocator.get))
    }

    val sessionIdGenerator = SessionIdGenerator(livyConf, sessionStore, clusterManager, zkManager)
    val batchSessionManager = new BatchSessionManager(livyConf, sessionStore, sessionIdGenerator)
    val interactiveSessionManager = new InteractiveSessionManager(livyConf, sessionStore,
      sessionIdGenerator)

    server = new WebServer(livyConf, host, port)
    server.context.setResourceBase("src/main/org/apache/livy/server")

    val livyVersionServlet = new JsonServlet {
      before() { contentType = "application/json" }

      get("/") {
        Map("version" -> LIVY_VERSION,
          "user" -> LIVY_BUILD_USER,
          "revision" -> LIVY_REVISION,
          "branch" -> LIVY_BRANCH,
          "date" -> LIVY_BUILD_DATE,
          "url" -> LIVY_REPO_URL)
      }
    }

    // Servlet for hosting static files such as html, css, and js
    // Necessary since Jetty cannot set it's resource base inside a jar
    // Returns 404 if the file does not exist
    val staticResourceServlet = new ScalatraServlet {
      get("/*") {
        val fileName = params("splat")
        val notFoundMsg = "File not found"

        if (!fileName.isEmpty) {
          getClass.getResourceAsStream(s"ui/static/$fileName") match {
            case is: InputStream => new BufferedInputStream(is)
            case null => NotFound(notFoundMsg)
          }
        } else {
          NotFound(notFoundMsg)
        }
      }
    }

    def uiRedirectServlet(path: String) = new ScalatraServlet {
      get("/") {
        redirect(path)
      }
    }

    server.context.addEventListener(
      new ServletContextListener() with MetricsBootstrap with ServletApiImplicits {

        private def mount(sc: ServletContext, servlet: Servlet, mappings: String*): Unit = {
          val registration = sc.addServlet(servlet.getClass().getName(), servlet)
          registration.addMapping(mappings: _*)
          registration.setMultipartConfig(multipartConfig)
        }

        override def contextDestroyed(sce: ServletContextEvent): Unit = {

        }

        override def contextInitialized(sce: ServletContextEvent): Unit = {
          try {
            val context = sce.getServletContext()
            context.initParameters(org.scalatra.EnvironmentKey) = livyConf.get(ENVIRONMENT)

            val interactiveServlet = new InteractiveSessionServlet(
              interactiveSessionManager, sessionAllocator, clusterManager,
              sessionStore, livyConf, accessManager)
            mount(context, interactiveServlet, "/sessions/*")

            val batchServlet =
              new BatchSessionServlet(batchSessionManager, sessionAllocator, clusterManager,
                sessionStore, livyConf, accessManager)
            mount(context, batchServlet, "/batches/*")

            val hdfsServlet =
              new HdfsServlet(livyConf)
            mount(context, hdfsServlet, "/hdfs/*")

            if (livyConf.getBoolean(UI_ENABLED)) {
              val uiServlet = new UIServlet(basePath, livyConf)
              mount(context, uiServlet, "/ui/*")
              mount(context, staticResourceServlet, "/static/*")
              mount(context, uiRedirectServlet(basePath + "/ui/"), "/*")
              _thriftServerFactory.foreach { factory =>
                mount(context, factory.getServlet(basePath), factory.getServletMappings: _*)
              }
            } else {
              mount(context, uiRedirectServlet(basePath + "/metrics"), "/*")
            }

            val jmxJsonServlet = new JmxJsonServlet()
            mount(context, jmxJsonServlet, "/jmx")

            context.mountMetricsAdminServlet("/metrics")

            mount(context, livyVersionServlet, "/version/*")
          } catch {
            case e: Throwable =>
              error("Exception thrown when initializing server", e)
              sys.exit(1)
          }
        }

      })

    livyConf.get(AUTH_TYPE) match {
      case authType @ KerberosAuthenticationHandler.TYPE =>
        val principal = SecurityUtil.getServerPrincipal(livyConf.get(AUTH_KERBEROS_PRINCIPAL),
          server.host)
        val keytab = livyConf.get(AUTH_KERBEROS_KEYTAB)
        require(principal != null,
          s"Kerberos auth requires ${AUTH_KERBEROS_PRINCIPAL.key} to be provided.")
        require(keytab != null,
          s"Kerberos auth requires ${AUTH_KERBEROS_KEYTAB.key} to be provided.")

        val holder = new FilterHolder(new AuthenticationFilter())
        holder.setInitParameter(AuthenticationFilter.AUTH_TYPE, authType)
        holder.setInitParameter(KerberosAuthenticationHandler.PRINCIPAL, principal)
        holder.setInitParameter(KerberosAuthenticationHandler.KEYTAB, keytab)
        holder.setInitParameter(KerberosAuthenticationHandler.NAME_RULES,
          livyConf.get(AUTH_KERBEROS_NAME_RULES))
        server.context.addFilter(holder, "/*", EnumSet.allOf(classOf[DispatcherType]))
        info(s"SPNEGO auth enabled (principal = $principal)")

      case authType @ LdapAuthenticationHandlerImpl.TYPE =>
        val holder = new FilterHolder(new AuthenticationFilter())
        holder.setInitParameter(AuthenticationFilter.AUTH_TYPE,
          LdapAuthenticationHandlerImpl.getClass.getCanonicalName.dropRight(1))
        Option(livyConf.get(LivyConf.AUTH_LDAP_URL)).foreach { url =>
          holder.setInitParameter(LdapAuthenticationHandlerImpl.PROVIDER_URL, url)
        }
        Option(livyConf.get(LivyConf.AUTH_LDAP_USERNAME_DOMAIN)).foreach { domain =>
          holder.setInitParameter(LdapAuthenticationHandlerImpl.LDAP_BIND_DOMAIN, domain)
        }
        Option(livyConf.get(LivyConf.AUTH_LDAP_BASE_DN)).foreach { baseDN =>
          holder.setInitParameter(LdapAuthenticationHandlerImpl.BASE_DN, baseDN)
        }
        holder.setInitParameter(LdapAuthenticationHandlerImpl.SECURITY_AUTHENTICATION,
          livyConf.get(LivyConf.AUTH_LDAP_SECURITY_AUTH))
        holder.setInitParameter(LdapAuthenticationHandlerImpl.ENABLE_START_TLS,
          livyConf.get(LivyConf.AUTH_LDAP_ENABLE_START_TLS))
        server.context.addFilter(holder, "/*", EnumSet.allOf(classOf[DispatcherType]))
        info("LDAP auth enabled.")

      case null =>
        // Nothing to do.

      case customType =>
        val authClassConf = s"livy.server.auth.$customType.class"
        val authClass = livyConf.get(authClassConf)
        require(authClass != null, s"$customType auth requires $authClassConf to be provided")

        val holder = new FilterHolder()
        holder.setClassName(authClass)

        val prefix = s"livy.server.auth.$customType.param."
        livyConf.asScala.filter { kv =>
          kv.getKey.length > prefix.length && kv.getKey.startsWith(prefix)
        }.foreach { kv =>
          holder.setInitParameter(kv.getKey.substring(prefix.length), kv.getValue)
        }
        server.context.addFilter(holder, "/*", EnumSet.allOf(classOf[DispatcherType]))
        info(s"$customType auth enabled")
    }

    if (livyConf.getBoolean(CSRF_PROTECTION)) {
      info("CSRF protection is enabled.")
      val csrfHolder = new FilterHolder(new CsrfFilter())
      server.context.addFilter(csrfHolder, "/*", EnumSet.allOf(classOf[DispatcherType]))
    }

    if (accessManager.isAccessControlOn) {
      info("Access control is enabled")
      val accessHolder = new FilterHolder(new AccessFilter(accessManager))
      server.context.addFilter(accessHolder, "/*", EnumSet.allOf(classOf[DispatcherType]))
    }

    server.start()
    clusterManager.foreach(_.register())

    _thriftServerFactory.foreach {
      _.start(livyConf, interactiveSessionManager, sessionStore, accessManager)
    }

    Runtime.getRuntime().addShutdownHook(new Thread("Livy Server Shutdown") {
      override def run(): Unit = {
        info("Shutting down Livy server.")
        zkManager.foreach(_.stop())
        server.stop()
        _thriftServerFactory.foreach(_.stop())
      }
    })

    _serverUrl = Some(s"${server.protocol}://${server.host}:${server.port}")
    sys.props("livy.server.server-url") = _serverUrl.get
  }

  def runKinit(keytab: String, principal: String): Boolean = {
    val commands = Seq("kinit", "-kt", keytab, principal)
    val proc = new ProcessBuilder(commands: _*).inheritIO().start()
    proc.waitFor() match {
      case 0 =>
        debug("Ran kinit command successfully.")
        kinitFailCount = 0
        true
      case _ =>
        warn("Fail to run kinit command.")
        kinitFailCount += 1
        false
    }
  }

  def startKinitThread(keytab: String, principal: String): Unit = {
    val refreshInterval = livyConf.getTimeAsMs(LAUNCH_KERBEROS_REFRESH_INTERVAL)
    val kinitFailThreshold = livyConf.getInt(KINIT_FAIL_THRESHOLD)
    executor.schedule(
      new Runnable() {
        override def run(): Unit = {
          if (runKinit(keytab, principal)) {
            // The current UGI should never change. If that happens, it is an error condition and
            // relogin the original UGI would not update the current UGI. So the server will fail
            // due to no valid credentials. The assert here allows to fast detect this error
            // condition and fail immediately with a meaningful error.
            assert(ugi.equals(UserGroupInformation.getCurrentUser), "Current UGI has changed.")
            ugi.reloginFromTicketCache()
            // schedule another kinit run with a fixed delay.
            executor.schedule(this, refreshInterval, TimeUnit.MILLISECONDS)
          } else {
            // schedule another retry at once or fail the livy server if too many times kinit fail
            if (kinitFailCount >= kinitFailThreshold) {
              error(s"Exit LivyServer after ${kinitFailThreshold} times failures running kinit.")
              if (server.server.isStarted()) {
                stop()
              } else {
                sys.exit(1)
              }
            } else {
              executor.submit(this)
            }
          }
        }
      }, refreshInterval, TimeUnit.MILLISECONDS)
  }

  def join(): Unit = server.join()

  def stop(): Unit = {
    if (server != null) {
      server.stop()
    }
    if (sessionStagingDirManager != null) {
      sessionStagingDirManager.stop()
    }
    Metrics.close()
  }

  def serverUrl(): String = {
    _serverUrl.getOrElse(throw new IllegalStateException("Server not yet started."))
  }

  /** For ITs only */
  def getJdbcUrl: Option[String] = {
    _thriftServerFactory.map { _ =>
      val additionalUrlParams = if (livyConf.get(THRIFT_TRANSPORT_MODE) == "http") {
        "?hive.server2.transport.mode=http;hive.server2.thrift.http.path=cliservice"
      } else {
        ""
      }
      val host = Option(livyConf.get(THRIFT_BIND_HOST)).getOrElse(
        InetAddress.getLocalHost.getHostAddress)
      val port = livyConf.getInt(THRIFT_SERVER_PORT)
      s"jdbc:hive2://$host:$port$additionalUrlParams"
    }
  }

  private[livy] def testRecovery(livyConf: LivyConf): Unit = {
    if (!livyConf.isRunningOnYarn()) {
      // If recovery is turned on but we are not running on YARN, quit.
      require(livyConf.get(LivyConf.RECOVERY_MODE) == SESSION_RECOVERY_MODE_OFF,
        "Session recovery requires YARN.")
    }
  }
}

object LivyServer {

  def main(args: Array[String]): Unit = {
    val server = new LivyServer()
    try {
      server.start()
      server.join()
    } finally {
      server.stop()
    }
  }

}
