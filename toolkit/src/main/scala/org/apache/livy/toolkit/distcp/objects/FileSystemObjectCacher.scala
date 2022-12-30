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

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import org.apache.livy.toolkit.distcp.utils.PathUtils

/**
 * FileSystem caching class. Aims to prevent many of the same FileSystem
 * objects being created between copy and delete actions in the same partition.
 */
class FileSystemObjectCacher(hadoopConfiguration: Configuration) {

  private val cache: mutable.Map[URI, FileSystem] = mutable.Map.empty

  /**
   * Get a FileSystem object based on the given URI if it already exists. If it
   * doesn't exist, create one and store it.
   */
  def getOrCreate(uri: URI): FileSystem = get(uri) match {
    case Some(fs) => fs
    case None =>
      val fs = FileSystem.get(uri, hadoopConfiguration)
      cache.update(fs.getUri, fs)
      fs
  }

  /**
   * Get a FileSystem object based on the given URI if it already exists.
   */
  def get(uri: URI): Option[FileSystem] = cache.collectFirst {
    case (u, f) if PathUtils.uriIsChild(u.resolve("/"), uri) => f
  }
}
