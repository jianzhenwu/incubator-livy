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

import java.io.File

import org.scalatra.{BadRequest, ContentEncodingSupport, InternalServerError, MethodOverride, Ok, UrlGeneratorSupport}

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.server.{ApiVersioningSupport, JsonServlet}

object HdfsServlet extends Logging

class HdfsServlet(
    livyConf: LivyConf)
  extends JsonServlet
    with ApiVersioningSupport
    with MethodOverride
    with UrlGeneratorSupport
    with ContentEncodingSupport {

  val HDFS_COMMAND_REGEX = "^(hadoop fs|hadoop dfs|hdfs dfs) (.*)".r

  error {
    case e: IllegalArgumentException =>
      HdfsServlet.warn("bad request: ", e)
      BadRequest(ResponseMessage(e.getMessage))
    case e =>
      HdfsServlet.error("internal error", e)
      InternalServerError(e.toString)
  }

  jpost[HdfsCommandRequest]("/cmd") { req =>
    val cmd = req.cmd.trim

    val runnableCmd = if (!LivyConf.TEST_MODE) {
      cmd match {
        case HDFS_COMMAND_REGEX(_, _) =>
        case _ => throw new IllegalArgumentException("Invalid operation: " + cmd)
      }

      sys.env.getOrElse("HADOOP_HOME",
        throw new Exception("HADOOP_HOME env not found")) +
        File.separator + "bin" + File.separator + cmd
    } else {
      cmd
    }

    val builder = new HdfsCmdProcessBuilder(livyConf)
    builder.username(request.getRemoteUser)
    val hdfsProcess = builder.start(runnableCmd)

    val exitCode = hdfsProcess.waitFor()
    exitCode match {
      case 0 =>
      case exitCode =>
        HdfsServlet.warn(s"hadoop exited with code $exitCode")
    }

    val output = hdfsProcess.inputIterator.mkString("\n")
    val err = hdfsProcess.errorIterator.mkString("\n")

    val res = HdfsCommandResponse(exitCode, output, err)
    Ok(res)
  }
}

