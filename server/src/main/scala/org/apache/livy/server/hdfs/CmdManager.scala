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

import scala.sys.process.{Process, ProcessIO}

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.server.auth.HttpBasicAuthenticationHolder

class CmdManager(livyConf: LivyConf) extends Logging {
  def run(req: HdfsCommandRequest, curDir: String): HdfsCommandResponse = {
    info("executing dir: " + curDir + " command: " + req.cmd)
    val stdoutLen = livyConf.getInt(LivyConf.HDFS_COMMAND_STDOUT_MAX_MESSAGE_SIZE)
    val stderrLen = livyConf.getInt(LivyConf.HDFS_COMMAND_STDERR_MAX_MESSAGE_SIZE)
    var outStr = new Array[Char](stdoutLen)
    var errStr = new Array[Char](stderrLen)

    // TODO Temp solution, refactor to DMP auth and extract shopee related code later
    val envs = collection.mutable.Map[String, String]()
    if (livyConf.getBoolean(LivyConf.DESIGNATION_ENABLED)) {
      HttpBasicAuthenticationHolder.get().fold {
        envs("HADOOP_USER_NAME") = ""
        envs("HADOOP_USER_RPCPASSWORD") = ""
      } { case (username, password) =>
        envs("HADOOP_USER_NAME") = username
        envs("HADOOP_USER_RPCPASSWORD") = password
      }
    }

    val pb = Process(req.cmd.mkString, new java.io.File(curDir), envs.toSeq: _*)
    val pio = new ProcessIO(_ => (),
      stdout =>
        scala.io.Source.fromInputStream(stdout).copyToArray(outStr, 0, stdoutLen),
      stderr =>
        scala.io.Source.fromInputStream(stderr).copyToArray(errStr, 0, stderrLen)
    )

    val re = pb.run(pio)
    HdfsCommandResponse(re.exitValue(), outStr.mkString, errStr.mkString)
  }

}
