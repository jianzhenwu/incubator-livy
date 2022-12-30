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
package org.apache.livy.toolkit.distcp.objects

import java.net.URI

import org.apache.hadoop.fs.{FileStatus, Path}

/**
 * Case class to represent a simple status of a File. Exists because
 * [[FileStatus]] is not serializable.
 */
case class SerializableFileStatus(uri: URI, len: Long, fileType: FileType)
  extends Serializable {
  def getPath: Path = new Path(uri)

  def getLen: Long = len

  def isDirectory: Boolean = fileType == Directory

  def isFile: Boolean = fileType == File
}

object SerializableFileStatus {

  /**
   * Create a [[SerializableFileStatus]] from a [[FileStatus]] object.
   */
  def apply(fileStatus: FileStatus): SerializableFileStatus = {
    val fileType = if (fileStatus.isDirectory) {
      Directory
    } else if (fileStatus.isFile) {
      File
    } else {
      throw new RuntimeException(s"File [$fileStatus] is neither a directory or file")
    }
    new SerializableFileStatus(
      fileStatus.getPath.toUri,
      fileStatus.getLen,
      fileType
    )
  }
}

sealed trait FileType extends Serializable

case object File extends FileType

case object Directory extends FileType
