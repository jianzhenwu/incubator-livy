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

import org.scalatra.{BadRequest, ContentEncodingSupport, InternalServerError, MethodOverride, Ok, UrlGeneratorSupport}

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.server.{ApiVersioningSupport, JsonServlet}

object HdfsServlet extends Logging

class HdfsServlet(
    livyConf: LivyConf,
    cmdManager: CmdManager)
  extends JsonServlet
    with ApiVersioningSupport
    with MethodOverride
    with UrlGeneratorSupport
    with ContentEncodingSupport {

  error {
    case e: IllegalArgumentException =>
      HdfsServlet.warn("bad request: ", e)
      BadRequest(ResponseMessage(e.getMessage))
    case e =>
      HdfsServlet.error("internal error", e)
      InternalServerError(e.toString)
  }

  def validation(req: HdfsCommandRequest): Unit = {
    val cmd = req.cmd.mkString.trim

    require(req.cmd != "", "No command provided.")
    require(cmd.startsWith("hadoop fs") || cmd.startsWith("hdfs"), "Invalid operation: " + cmd)
  }

  jpost[HdfsCommandRequest]("/cmd") { req =>
    validation(req)

    val hadoopHome = sys.env.get("HADOOP_HOME")
    val res = cmdManager.run(req, hadoopHome.mkString)
    Ok(res)
  }
}

