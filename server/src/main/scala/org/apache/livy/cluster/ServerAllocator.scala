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

package org.apache.livy.cluster

import scala.reflect.ClassTag

import org.apache.livy.LivyConf
import org.apache.livy.sessions.Session.RecoveryMetadata

class ServerAllocator(livyConf: LivyConf,
                      clusterManager: ClusterManager) {

  def allocateServer[T <: RecoveryMetadata](
      sessionType: String,
      sessionId: Int,
      owner: String) (implicit t: ClassTag[T]): Set[ServerNode] = {

    val serverSet = livyConf.serverMapping.getOrElse(owner, Set[String]())

    val serverNodes = clusterManager.getNodes();
    val serverAllocated = livyConf.serverMapping.values.flatten.toSet[String]

    serverNodes.filter(
      if (serverSet.nonEmpty) {
        e => serverSet.contains(s"${e.host}:${e.port}")
      } else {
        e => !serverAllocated.contains(s"${e.host}:${e.port}")
      }
    )
  }
}
