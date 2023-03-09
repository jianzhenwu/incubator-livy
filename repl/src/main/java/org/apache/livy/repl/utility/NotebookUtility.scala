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

package org.apache.livy.repl.utility

import java.io.{File, IOException}

import org.apache.commons.io.IOUtils

class NotebookUtility extends Utility {

  override def name() = "notebook"

  lazy val workspaceUri = System.getenv("NOTEBOOK_ALLUXIO_WORKSPACE_URI")
  lazy val userDir = System.getProperty("user.dir")
  lazy val logDir = System.getProperty("spark.yarn.app.container.log.dir")

  def mountWorkspace(): String = {
    if (null == workspaceUri || workspaceUri.isEmpty) {
      throw new RuntimeException("notebook workspace uri is null or empty.")
    }
    val p1 = createAlluxioMountProcess(
      "$ALLUXIO_FUSE_HOME/alluxio-fuse mount /notebook/dataset %s/dataset"
          .format(workspaceUri))
    val res1 = getProcessOutputAsString(p1)
    if (p1.waitFor() == 0) {
      val cmd =
        """
            |start_mount_time=`date +%s`;
            |while true
            |   do
            |     now_time=`date +%s`;
            |     mount_spent_time=$(($now_time - $start_mount_time));
            |   if [ $(mount|grep alluxio-fuse|wc -l) -gt 0 ]; then
            |     echo mount success
            |     break;
            |   elif [ $mount_spent_time -gt 15 ]; then
            |     echo mount over time
            |     exit -1;
            |   else sleep 1;
            |   fi;
            |   done;
            |""".stripMargin
      val p2 = createAlluxioMountProcess(cmd)
      val res2 = getProcessOutputAsString(p2)
      if (p2.waitFor() == 0) "mount successful." else "check mount failed. " + res2
    } else "exec mount failed. " + res1
  }

  private def getProcessOutputAsString(p: Process): String = {
    var res = ""
    try {
      res = IOUtils.toString(p.getInputStream)
    } catch {
      case e: IOException => res += "IOException: %s".format(e.getMessage)
    }
    res
  }

  private def createAlluxioMountProcess(cmd: String): Process = {
    if (null == logDir || logDir.isEmpty) {
      throw new RuntimeException("Can't find driver log dir.")
    }
    if (null == userDir || userDir.isEmpty) {
      throw new RuntimeException("Can't find driver work dir.")
    }
    val builder = new ProcessBuilder("sh", "-c", cmd).redirectErrorStream(true)

    val env = builder.environment()
    val alluxioHome = getAlluxioHomePath()
    env.put("ALLUXIO_LOGS_DIR", logDir)
    env.put("ALLUXIO_FUSE_HOME", s"$alluxioHome/integration/fuse/bin/")
    env.put("CLASSPATH",
      s"$alluxioHome/alluxio/integration/fuse/*.jar")
    builder.start()
  }

  private def getAlluxioHomePath(): String = {
    val dir = new File(s"$userDir/alluxio")
    dir.listFiles().find(x => {x.isDirectory && x.getName.startsWith("alluxio")})
        .get.getAbsolutePath
  }
}