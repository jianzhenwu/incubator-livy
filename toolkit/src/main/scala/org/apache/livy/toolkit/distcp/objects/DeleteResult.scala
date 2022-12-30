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
 * Result of the DistCp delete used for both logging to a logger and a file.
 */
case class DeleteResult(path: URI, actionResult: DeleteActionResult)
  extends DistCpResult {
  def getMessage: String =
    s"Path: [$path], Type: [Delete], Result: [${actionResult.message}]"
}

sealed trait DeleteActionResult extends Serializable {
  def message: String = this.getClass.getSimpleName
}

object DeleteActionResult {

  object SkippedDoesNotExists extends DeleteActionResult

  object SkippedDryRun extends DeleteActionResult

  object Deleted extends DeleteActionResult

  case class Failed(e: Throwable) extends DeleteActionResult {
    override def message: String = s"${super.message}: ${e.getMessage}"
  }
}
