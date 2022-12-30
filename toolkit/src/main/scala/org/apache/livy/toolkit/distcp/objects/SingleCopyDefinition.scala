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

import org.apache.livy.toolkit.distcp.SparkDistCpBootstrap.KeyedCopyDefinition

/**
 * Definition of a single copy.
 *
 * @param source Source file/folder to copy
 * @param destination Destination to copy to
 */
case class SingleCopyDefinition(
     source: SerializableFileStatus,
     destination: URI)

/**
 * Definition of a copy that includes any copying of parent folders this
 * file/folder depends on.
 *
 * @param source Source file/folder to copy
 * @param destination Destination to copy to
 * @param dependentFolders Any dependent folder copies this file/folder depends on
 */
case class CopyDefinitionWithDependencies(
     source: SerializableFileStatus,
     destination: URI,
     dependentFolders: Seq[SingleCopyDefinition]) {

  def toKeyedDefinition: KeyedCopyDefinition = (destination, this)

  def getAllCopyDefinitions: Seq[SingleCopyDefinition] =
    dependentFolders :+ SingleCopyDefinition(source, destination)
}
