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

package org.apache.livy.server.event

import org.apache.livy.LivyConf

object Events {
  @volatile
  private var eventDispatcher: EventDispatcher = _

  def init(livyConf: LivyConf): Unit = {
    if (eventDispatcher == null) {
      this.synchronized {
        if (eventDispatcher == null) {
          eventDispatcher = new EventDispatcher(livyConf)
          eventDispatcher.start()
        }
      }
    }
  }

  def close(): Unit = {
    this.synchronized {
      if (eventDispatcher != null) {
        eventDispatcher.stop()
        eventDispatcher = null
      }
    }
  }

  def apply(): Events = {
    if (eventDispatcher == null) {
      throw new IllegalStateException("Events not initialized yet!")
    } else {
      eventDispatcher
    }
  }
}

trait Events {
  def notify(event: Event): Unit
}
