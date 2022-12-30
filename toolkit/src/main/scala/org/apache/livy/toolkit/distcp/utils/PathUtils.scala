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
package org.apache.livy.toolkit.distcp.utils

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object PathUtils {

  /**
   * Qualify a path, making the path both absolute and qualifies with a scheme.
   * If the input path is not absolute, the default working directory is used.
   * If the input path does not have a scheme, the default URI used in the
   * Hadoop Configuration is used.
   */
  def pathToQualifiedPath(
      hadoopConfiguration: Configuration,
      path: Path): Path = {
    val fs = FileSystem.get(hadoopConfiguration)
    path.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }

  /**
   * Transform a source input path URI into a destination path URI. This
   * function determines how a source file path is mapped to the destination.
   * The behaviour is different depending on if update or overwrite is used.
   * This follows the behaviour of Hadoop DistCp. See the Hadoop DistCp
   * documentation for more explanation of this behaviour.
   *
   * @param file URI of source file
   * @param sourceURI URI of root copy folder on source FileSystem
   * @param destinationURI URI of root copy folder on destination FileSystem
   * @param updateOverwritePathBehaviour Whether to use the overwrite/update path behaviour
   * @return Source file path URI mapped to the destination FileSystem
   */
  def sourceURIToDestinationURI(
      file: URI,
      sourceURI: URI,
      destinationURI: URI,
      updateOverwritePathBehaviour: Boolean): URI = {
    val sourceFolderURI: URI = {
      if (updateOverwritePathBehaviour) {
        sourceURI
      } else {
        Option(new Path(sourceURI).getParent).map(_.toUri).getOrElse(sourceURI)
      }
    }
    val relativeFile = sourceFolderURI.relativize(file).getPath
    new Path(new Path(destinationURI), relativeFile).toUri
  }

  /**
   * Check whether one URI is the parent of another URI.
   */
  def uriIsChild(parent: URI, child: URI): Boolean = {
    if (!parent.isAbsolute || !child.isAbsolute) {
      throw new RuntimeException(
        s"The URIs $parent and $child must have a scheme."
      )
    } else if (!parent.getPath.startsWith("/") || !child.getPath.startsWith("/")) {
      throw new RuntimeException(
        s"The URIs $parent and $child must have an absolute path."
      )
    } else {
      parent.relativize(child) != child
    }
  }
}
