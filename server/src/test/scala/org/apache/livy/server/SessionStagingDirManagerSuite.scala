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

import java.io.{File, FileNotFoundException}
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}

class SessionStagingDirManagerSuite extends FunSuite with BeforeAndAfterAll
  with LivyBaseUnitTestSuite {

  private var livyConf: LivyConf = _
  private var fs: FileSystem = _
  private var sessionStagingDirManager: SessionStagingDirManager = _

  private var serverStagingTempDir: File = _
  private var clientStagingTempDir: File = _

  override def beforeAll(): Unit = {
    livyConf = new LivyConf()
  }

  override def afterAll(): Unit = {
    if (sessionStagingDirManager != null) {
      sessionStagingDirManager.stop()
    }
  }

  test("start session staging directory manager") {
    sessionStagingDirManager = new SessionStagingDirManager(livyConf)
    assertThrows[IllegalArgumentException] {
      sessionStagingDirManager.start()
    }
    livyConf.set(LivyConf.SESSION_STAGING_DIR, "tmp")
    sessionStagingDirManager = new SessionStagingDirManager(livyConf)
    assertThrows[FileNotFoundException] {
      sessionStagingDirManager.start()
    }
  }

  test("start remove session staging file") {
    serverStagingTempDir = Files.createTempDirectory(".server_livy_session").toFile
    clientStagingTempDir = Files.createTempDirectory(".client_livy_session").toFile

    livyConf.set(LivyConf.SESSION_STAGING_DIR_MAX_AGE, "100ms")
    livyConf.set(LivyConf.SESSION_STAGING_DIR, serverStagingTempDir.getAbsolutePath)
    livyConf.set(LivyConf.LIVY_LAUNCHER_SESSION_STAGING_DIR, clientStagingTempDir.getAbsolutePath)

    File.createTempFile(".livy_session_", "file1", serverStagingTempDir)
    assert(serverStagingTempDir.listFiles().length == 1)
    File.createTempFile(".livy_session_", "file2", clientStagingTempDir)
    assert(clientStagingTempDir.listFiles().length == 1)

    fs = FileSystem.newInstance(new Configuration())
    sessionStagingDirManager = new SessionStagingDirManager(livyConf)
    sessionStagingDirManager.start()
    Thread.sleep(livyConf.getTimeAsMs(LivyConf.SESSION_STAGING_DIR_MAX_AGE) + 5 * 1000)
    assert(serverStagingTempDir.listFiles.length == 0)
    assert(clientStagingTempDir.listFiles.length == 0)
  }

  test("should recursive remove session staging file") {
    serverStagingTempDir = Files.createTempDirectory(".server_livy_session").toFile
    clientStagingTempDir = Files.createTempDirectory(".client_livy_session").toFile
    val serverStagingDir = Files.createDirectories(
      new File(serverStagingTempDir, "tmp").toPath).toFile
    val clientStagingDir = Files.createDirectories(
      new File(clientStagingTempDir, "tmp").toPath).toFile
    assert(serverStagingDir.isDirectory)
    assert(clientStagingDir.isDirectory)

    livyConf.set(LivyConf.SESSION_STAGING_DIR_MAX_AGE, "100ms")
    livyConf.set(LivyConf.SESSION_STAGING_DIR, serverStagingTempDir.getAbsolutePath)
    livyConf.set(LivyConf.LIVY_LAUNCHER_SESSION_STAGING_DIR, clientStagingTempDir.getAbsolutePath)

    File.createTempFile(".livy_session_", "file1", serverStagingDir)
    assert(serverStagingDir.listFiles().length == 1)
    File.createTempFile(".livy_session_", "file2", clientStagingDir)
    assert(clientStagingDir.listFiles().length == 1)

    fs = FileSystem.newInstance(new Configuration())
    sessionStagingDirManager = new SessionStagingDirManager(livyConf)
    sessionStagingDirManager.start()
    Thread.sleep(livyConf.getTimeAsMs(LivyConf.SESSION_STAGING_DIR_MAX_AGE) + 5 * 1000)
    assert(!serverStagingDir.exists())
    assert(!clientStagingDir.exists())
  }
}
