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

package org.apache.livy.server.hdfs

import scala.collection.mutable.ArrayBuffer

import org.apache.livy.{ApplicationEnvProcessor, LivyConf, Logging}
import org.apache.livy.utils.LineBufferedProcess

class HdfsCmdProcessBuilder(livyConf: LivyConf) extends Logging {
  private[this] var _env: ArrayBuffer[(String, String)] = ArrayBuffer()
  private[this] var _username: String = ""

  private[this] val applicationEnvProcessor: ApplicationEnvProcessor =
    ApplicationEnvProcessor(livyConf.get(LivyConf.APPLICATION_ENV_PROCESSOR))

  def env(key: String, value: String): HdfsCmdProcessBuilder = {
    _env += ((key, value))
    this
  }

  def username(username: String): HdfsCmdProcessBuilder = {
    _username = username
    this
  }

  def start(cmd: String): LineBufferedProcess = {
    info(s"executing ${cmd}")

    val pb = new ProcessBuilder("/bin/sh", "-c", cmd)

    val env = pb.environment()
    applicationEnvProcessor.process(env, _username)

    pb.redirectErrorStream(true)
    pb.redirectInput(ProcessBuilder.Redirect.PIPE)

    new LineBufferedProcess(pb.start(),
      livyConf.getInt(LivyConf.HDFS_COMMAND_LOGS_SIZE))
  }
}
