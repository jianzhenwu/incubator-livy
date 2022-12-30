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

/**
 * Result of the DistCp copy used for both logging to a logger and a file.
 */
trait CopyResult extends DistCpResult

case class FileCopyResult(
    source: URI,
    destination: URI,
    len: Long,
    copyAction: FileCopyActionResult) extends CopyResult {
  def getMessage: String = s"Source: $source, Destination: $destination, " +
    s"Type: FileCopy: $len bytes, Result: ${copyAction.message}"
}

case class DirectoryCopyResult(
     source: URI,
     destination: URI,
     copyAction: DirectoryCreateActionResult) extends CopyResult {
  def getMessage: String =
    s"Source: $source, Destination: $destination, Type: DirectoryCreate," +
      s"Result: ${copyAction.message}"
}

sealed trait CopyActionResult extends Serializable {
  def message: String = this.getClass.getSimpleName.stripSuffix("$")
}

sealed trait FileCopyActionResult extends CopyActionResult

sealed trait DirectoryCreateActionResult extends CopyActionResult

object CopyActionResult {

  object SkippedAlreadyExists extends FileCopyActionResult with DirectoryCreateActionResult

  object SkippedIdenticalFileAlreadyExists extends FileCopyActionResult

  object SkippedDryRun extends FileCopyActionResult with DirectoryCreateActionResult

  object Created extends DirectoryCreateActionResult

  object Copied extends FileCopyActionResult

  object OverwrittenOrUpdated extends FileCopyActionResult

  case class Failed(e: Throwable) extends FileCopyActionResult with DirectoryCreateActionResult {
    override def message: String = s"${super.message}: ${e.getMessage}"
  }
}

