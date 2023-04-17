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

import java.io.File

import com.google.common.io.Files
import org.scalatest.{BeforeAndAfter, FunSpec}

class FsUtilitySpec extends FunSpec with BeforeAndAfter {

  private var utils: FsUtility = _
  private var tempDir: File = _
  private var tempFiles: Seq[File] = Nil

  before {
    utils = new FsUtility()
    tempDir = Files.createTempDir()
    tempFiles = List(
      File.createTempFile("test1", ".txt", tempDir),
      File.createTempFile("test2", ".txt", tempDir)
    )
  }

  describe("The Filesystem Utility") {

    it("should display help info of fs utility") {
      var output = utils.help("ls")
      assert(output.contains(
        "%utils fs.ls([-C] [-d] [-h] [-q] [-R] [-t] [-S] [-r] [-u] [<path> ...]) :"))

      output = utils.help("du")
      assert(output.contains(
        "%utils fs.du([-s] [-h] [-x] <path> ...) :"))
    }

    it("should list files") {
      val output = utils.ls(tempDir.getAbsolutePath)
      assert(output.contains("Found 2 items"))
      assert(output.contains(tempFiles.head.getAbsolutePath))
      assert(output.contains(tempFiles(1).getAbsolutePath))
    }

    it("should copy files") {
      val srcPath = tempFiles.head.getAbsolutePath
      val destPath = new File(tempDir, "test3.txt").getAbsolutePath

      utils.cp(s"$srcPath $destPath")
      val output = utils.ls(tempDir.getAbsolutePath)
      assert(output.contains("Found 3 items"))
      assert(output.contains(destPath))
    }

    it("should rename files") {
      val srcPath = tempFiles.head.getAbsolutePath
      val destPath = new File(tempDir, "test3.txt").getAbsolutePath

      utils.mv(s"$srcPath $destPath")
      val output = utils.ls(tempDir.getAbsolutePath)
      assert(output.contains("Found 2 items"))
      assert(output.contains(destPath))
    }

    it("should create new directory") {
      val newDir = tempDir + "/newDir"
      utils.mkdir(newDir)
      val output = utils.ls(tempDir.getAbsolutePath)
      assert(output.contains("Found 3 items"))
      assert(new File(newDir).isDirectory)
    }

    it("should remove directory") {
      val subDir = tempDir + "/dir"
      utils.mkdir(subDir)
      utils.rmr(subDir)
      val output = utils.ls(tempDir.getAbsolutePath)
      assert(output.contains("Found 2 items"))
      assert(!output.contains(subDir))
    }

    it("should remove file") {
      utils.rm(tempFiles.head.getAbsolutePath)
      var output = utils.ls(tempDir.getAbsolutePath)
      assert(output.contains("Found 1 items"))

      utils.rm(tempFiles(1).getAbsolutePath)
      output = utils.ls(tempDir.getAbsolutePath)
      assert(output.isEmpty)
    }

    it("should throw exception with invalid command") {
      assertThrows[ExecuteCommandException] {
        utils.help("other")
      }
      assertThrows[ExecuteCommandException] {
        utils.help("new")
      }
    }

    it("should finds all files that match the specified expression") {
      var output = utils.find(s"${tempDir.getAbsolutePath} -name ${tempFiles.head.getName}")
      assert(output ==
        s"""${tempFiles.head.getAbsolutePath}
           |""".stripMargin)

      output = utils.find(s"${tempDir.getAbsolutePath} -name ${tempFiles(1).getName}")
      assert(output ==
        s"""${tempFiles(1).getAbsolutePath}
           |""".stripMargin)
    }

    it("should checksum information for files") {
      val output = utils.checksum(s"${tempFiles.head.getAbsolutePath}")
      assert(output ==
        s"""${tempFiles.head.getAbsolutePath}\tNONE\t
           |""".stripMargin)
    }

    it("should displays the ACLs of files and directories") {
      val output = utils.getfacl(s"$tempDir")
      assert(output.contains(
        """user::rwx
          |group::r-x
          |other::r-x""".stripMargin))
    }

    it("should change the permissions of files") {
      var output = utils.getfacl(s"$tempDir")
      assert(output.contains(
        """user::rwx
          |group::r-x
          |other::r-x""".stripMargin))

      utils.chmod(s"-R 777 $tempDir")
      output = utils.getfacl(s"$tempDir")
      assert(output.contains(
        """user::rwx
          |group::rwx
          |other::rwx""".stripMargin))
    }

    it("should create a file of zero length") {
      val newFile = s"$tempDir/newFile"
      utils.touchz(newFile)
      val output = utils.ls(s"$tempDir")
      assert(output.contains("Found 3 items"))
      assert(output.contains(newFile))
    }
  }
}
