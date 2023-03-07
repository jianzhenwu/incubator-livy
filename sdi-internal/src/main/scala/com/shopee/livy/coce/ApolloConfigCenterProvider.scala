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

package com.shopee.livy.coce

import scala.collection.JavaConverters.asScalaSetConverter

import com.ctrip.framework.apollo.{Config, ConfigChangeListener, ConfigService}
import com.ctrip.framework.apollo.model.ConfigChangeEvent

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.server.coce.ConfigCenterProvider

object ApolloConfigCenterProvider {
  var mockConfigCenter: Config = _
}

class ApolloConfigCenterProvider(livyConf: LivyConf) extends ConfigCenterProvider with Logging {

  private val configCenter: Config = if (ApolloConfigCenterProvider.mockConfigCenter != null) {
    ApolloConfigCenterProvider.mockConfigCenter
  } else {
    // application namespace
    ConfigService.getAppConfig
  }

  private val interestedKeyPrefixes: Set[String] = Set(
    "livy.server.spark.version.alias.mapping",
    "livy.server.spark-home",
    "livy.server.spark-conf-dir",
    "livy.rsc.sparkr.package",
    "livy.rsc.pyspark.archives"
  )

  override def start(): Unit = {
    initConfigs()
    addChangeListener()
  }

  override def stop(): Unit = {}

  def isEffective(key: String, value: Any): Boolean = {
    interestedKeyPrefixes.exists(key.startsWith)
  }

  def initConfigs(): Unit = {
    val dynamicProperties = configCenter.getPropertyNames.asScala
    dynamicProperties.foreach(key => {
      val value = configCenter.getProperty(key, null)
      if (isEffective(key, value)) {
        livyConf.set(key, value)
        logger.info(s"Config center: Found property - key: $key, value: $value")
      }
    })
  }

  def addChangeListener(): Unit = {
    configCenter.addChangeListener(new ConfigChangeListener {
      override def onChange(changeEvent: ConfigChangeEvent): Unit = {
        logger.info("Config center: Changes for namespace " + changeEvent.getNamespace)
        changeEvent.changedKeys().asScala.foreach { key =>
          val change = changeEvent.getChange(key)
          if (isEffective(key, change.getNewValue)) {
            logger.info(s"Config center: Found change - key: ${change.getPropertyName}, " +
              s"oldValue: ${change.getOldValue}, newValue: ${change.getNewValue}, " +
              s"changeType: ${change.getChangeType}")
            if (change.getNewValue == null) {
              livyConf.remove(key)
            } else {
              livyConf.set(key, change.getNewValue)
            }
          }
        }
        livyConf.refreshCache()
      }
    })
  }
}
