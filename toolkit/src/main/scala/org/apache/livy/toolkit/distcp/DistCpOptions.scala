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
package org.apache.livy.toolkit.distcp

import java.io.IOException
import java.net.URI

import scala.util.matching.Regex

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

case class DistCpOptions(
     update: Boolean,
     overwrite: Boolean,
     delete: Boolean,
     log: Option[URI],
     ignoreErrors: Boolean,
     dryRun: Boolean,
     consistentPathBehaviour: Boolean,
     maxFilesPerTask: Int,
     maxBytesPerTask: Long,
     filters: Option[URI],
     filterNot: List[Regex],
     numListstatusThreads: Int) {

  val updateOverwritePathBehaviour: Boolean = !consistentPathBehaviour && (update || overwrite)

  def validateOptions(): Unit = {
    assert(maxFilesPerTask > 0, "maxFilesPerTask must be positive")
    assert(maxBytesPerTask > 0, "maxBytesPerTask must be positive")
    assert(numListstatusThreads > 0, "numListstatusThreads must be positive")
    assert(!(update && overwrite), "Both update and overwrite cannot be specified")
    assert(!(delete && !overwrite && !update),
      "Delete must be specified with either overwrite or update")
  }

  def withFiltersFromFile(hadoopConfiguration: Configuration): DistCpOptions = {
    val fn = filters.map(f => {
      try {
        val path = new Path(f)
        val fs = path.getFileSystem(hadoopConfiguration)
        val in = fs.open(path)
        val r = scala.io.Source.fromInputStream(in).getLines().map(_.r).toList
        in.close()
        r
      } catch {
        case e: IOException =>
          throw new RuntimeException("Invalid filter file " + f, e)
      }
    }).getOrElse(List.empty)
    this.copy(filterNot = fn)
  }
}

object DistCpOptions {

  def apply(): DistCpOptions = {
    DistCpOptions(
      Defaults.update,
      Defaults.overwrite,
      Defaults.delete,
      Defaults.log,
      Defaults.ignoreErrors,
      Defaults.dryRun,
      Defaults.consistentPathBehaviour,
      Defaults.maxFilesPerTask,
      Defaults.maxBytesPerTask,
      Defaults.filters,
      Defaults.filterNot,
      Defaults.numListstatusThreads
    )
  }

  object Defaults {
    val update: Boolean = false
    val overwrite: Boolean = false
    val delete: Boolean = false
    val log: Option[URI] = None
    val ignoreErrors: Boolean = false
    val dryRun: Boolean = false
    val consistentPathBehaviour: Boolean = false
    val maxFilesPerTask: Int = 1000
    val maxBytesPerTask: Long = 1073741824L
    val filters: Option[URI] = None
    val filterNot: List[Regex] = List.empty
    val numListstatusThreads: Int = 10
  }
}
