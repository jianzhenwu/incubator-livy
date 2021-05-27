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

import java.lang.reflect.{Constructor, InvocationTargetException}
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import com.google.common.base.Splitter
import com.google.common.collect.Lists

import org.apache.livy.{LivyConf, Logging}

class EventDispatcher(val livyConf: LivyConf) extends Events with Logging {

  private val eventListeners = initEventListeners(livyConf)

  private def initEventListeners(livyConf: LivyConf): List[EventListener] = {
    val eventListenerClasses: util.List[String] = Lists.newArrayList(Splitter.on(",")
      .trimResults.omitEmptyStrings.split(livyConf.get(LivyConf.EVENT_LISTENER_CLASSES)))
    if (eventListenerClasses.isEmpty) {
      return List.empty
    }

    val eventListeners = ListBuffer[EventListener]()
    for (eventListenerClass <- eventListenerClasses.asScala) {
      try {
        val clz: Class[_] = Class.forName(eventListenerClass)
        val constructor: Constructor[_] = clz.getConstructor(classOf[LivyConf])
        val eventListener = constructor.newInstance(livyConf).asInstanceOf[EventListener]
        eventListeners += eventListener
      }
      catch {
        case e@(_: NoSuchMethodException | _: InstantiationException
                | _: IllegalAccessException | _: InvocationTargetException
                | _: ClassNotFoundException) =>
          error("Unable to instantiate event listener class " + eventListenerClass, e)
          throw new IllegalArgumentException(e)
      }
    }
    eventListeners.toList
  }

  def start(): Unit = {
    eventListeners.foreach(_.start())
  }

  def notify(event: Event): Unit = {
    eventListeners.foreach(_.onEvent(event))
  }

  def stop(): Unit = {
    eventListeners.foreach(_.stop())
  }
}
