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

import java.io.{FileNotFoundException, IOException}
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.LivyConf._

class SessionStagingDirManager(livyConf: LivyConf) extends Logging {

  private val serverStagingDir = livyConf.get(SESSION_STAGING_DIR)

  private val clientStagingDir = livyConf.get(LIVY_LAUNCHER_SESSION_STAGING_DIR)

  private val maxAge = livyConf.getTimeAsMs(SESSION_STAGING_DIR_MAX_AGE)

  private val interval = livyConf.getTimeAsMs(SESSION_STAGING_DIR_CLEAN_INTERVAL)

  private val fs: FileSystem = FileSystem.newInstance(livyConf.hadoopConf)

  private lazy val executor = Executors.newScheduledThreadPool(1,
    new ThreadFactory() {
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r)
        thread.setName("session-staging-dir-clean-thread")
        thread.setDaemon(true)
        thread
      }
    })

  def start(): Unit = {
    if (serverStagingDir == null && clientStagingDir == null) {
      throw new IllegalArgumentException(
        s"Session staging directory is empty when" +
          s" livy.server.session.staging-dir.clean.enabled is true," +
          s" please set ${SESSION_STAGING_DIR.key} or" +
          s" ${LIVY_LAUNCHER_SESSION_STAGING_DIR.key}")
    }
    try {
      if (serverStagingDir != null &&
        !fs.getFileStatus(new Path(serverStagingDir)).isDirectory) {
        throw new IllegalArgumentException(
          s"Session staging directory ${SESSION_STAGING_DIR.key} " +
            s"is not a directory: $serverStagingDir.")
      }
      if (clientStagingDir != null &&
        !fs.getFileStatus(new Path(clientStagingDir)).isDirectory) {
        throw new IllegalArgumentException(
          s"Session staging directory ${LIVY_LAUNCHER_SESSION_STAGING_DIR.key} " +
            s"is not a directory: $serverStagingDir.")
      }
    } catch {
      case f: FileNotFoundException =>
        val msg = s"Session staging directory specified does not exist:" +
          s" $serverStagingDir or $clientStagingDir."
        throw new FileNotFoundException(msg).initCause(f)
    }
    startSessionStagingCleaner()
  }

  def stop(): Unit = {
    executor.shutdown()
    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
      executor.shutdownNow()
    }
    fs.close()
  }

  private def startSessionStagingCleaner(): Unit = {
    info(s"Start clean session staging directory.")
    executor.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = {
          if (serverStagingDir != null) {
            removeSessionFile(fs, serverStagingDir)
          }
          if (clientStagingDir != null) {
            removeSessionFile(fs, clientStagingDir)
          }
        }
      }, 5 * 1000, interval, TimeUnit.MILLISECONDS)
  }

  private def removeSessionFile(fs: FileSystem, stagingDir: String): Unit = {
    try {
      fs.listStatus(new Path(stagingDir))
        .filter(f => (System.currentTimeMillis() - f.getModificationTime) > maxAge)
        .foreach(f => {
          info(s"Deleting expired session file ${f.getPath}.")
          fs.delete(f.getPath, true)
        })
    } catch {
      case e: IOException =>
        error(s"Failed to clean session staging directory $stagingDir.", e);
    }
  }
}
