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

import javax.servlet.http.HttpServletResponse

import org.scalatest.BeforeAndAfterAll

import org.apache.livy.LivyConf
import org.apache.livy.server.BaseJsonServletSpec

class HdfsServletSpec
  extends BaseJsonServletSpec
    with BeforeAndAfterAll {
  protected def createConf(): LivyConf = {
    new LivyConf()
  }


  def createServlet(): HdfsServlet = {
    val livyConf = createConf()
    new HdfsServlet(livyConf)
  }

  protected val servlet = createServlet

  addServlet(servlet, "/hdfs/*")

  describe("HdfsServlet") {
    it("should run command successfully") {
      val hdfsCommand = new HdfsCommandRequest("ls /tmp")
      jpost[HdfsCommandResponse]("/hdfs/cmd", hdfsCommand, HttpServletResponse.SC_OK) { res =>
        res.exitCode should equal (0)
      }
    }
  }
}
