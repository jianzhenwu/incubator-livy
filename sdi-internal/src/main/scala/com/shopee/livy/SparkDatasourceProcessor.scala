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

package com.shopee.livy

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.shopee.livy.SparkDatasourceProcessor._

import org.apache.livy.{ApplicationEnvContext, ApplicationEnvProcessor, LivyConf}
import org.apache.livy.ApplicationEnvProcessor.SPARK_JARS
import org.apache.livy.utils.LivyProcessorException

/**
 * 1. Please add configurations below in livy/conf/spark-defaults.conf when using
 * this processor.
 *
 * spark.sql.catalog.hbase org.apache.spark.sql.execution.datasources.v2.hbase.HBaseCatalog
 * spark.sql.catalog.hbase.hive.metastore.uris thrift://hbase:9083
 * spark.sql.catalog.hbase.hive.metastore.warehouse.dir /user/hbase/warehouse
 * spark.sql.catalog.hbase.spark.sql.warehouse.dir /user/hive/warehouse/hbase.catalog
 *
 * spark.sql.catalog.jdbc org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCCatalog
 * spark.sql.catalog.jdbc.hive.metastore.uris thrift://jdbc:9083
 * spark.sql.catalog.jdbc.hive.metastore.warehouse.dir /user/jdbc/warehouse
 * spark.sql.catalog.jdbc.spark.sql.warehouse.dir /user/hive/warehouse/jdbc.catalog
 *
 * spark.sql.catalog.kafka org.apache.spark.sql.kafka010.KafkaCatalog
 * spark.sql.catalog.kafka.hive.metastore.uris thrift://kafka:9083
 * spark.sql.catalog.kafka.hive.metastore.warehouse.dir /user/kafka/warehouse
 * spark.sql.catalog.kafka.spark.sql.warehouse.dir /user/hive/warehouse/kafka.catalog
 *
 *
 * 2. Please add configurations below in livy/conf/livy.conf when using
 * this processor.
 *
 * livy.rsc.spark.sql.catalog.hbase.jars.v3.1 =
 * livy.rsc.spark.sql.catalog.jdbc.jars.v3.1 =
 * livy.rsc.spark.sql.catalog.kafka.jars.v3.1 =
 * livy.rsc.spark.sql.catalog.tfrecord.jars.v3.1 =
 *
 * livy.rsc.spark.sql.catalog.hbase.jars.v3.2 =
 * livy.rsc.spark.sql.catalog.jdbc.jars.v3.2 =
 * livy.rsc.spark.sql.catalog.kafka.jars.v3.2 =
 * livy.rsc.spark.sql.catalog.tfrecord.jars.v3.2 =
 */
object SparkDatasourceProcessor {

  val SPARK_SQL_DATASOURCE_CATALOG_IMPL = "spark.sql.datasource.catalog.impl"
  val catalogSet = Set("hive", "in-memory")

  val switches = new mutable.HashMap[String, String]()

  Seq("hbase", "jdbc", "kafka", "tfrecord").foreach { source =>
    switches.put(s"spark.sql.catalog.$source.enabled", source)
    switches.put(s"spark.livy.sql.catalog.$source.enabled", source)
  }
}

class SparkDatasourceProcessor extends ApplicationEnvProcessor {

  override def process(applicationEnvContext: ApplicationEnvContext): Unit = {
    val appConf = applicationEnvContext.appConf
    val catalogImpl = appConf.get(SPARK_SQL_DATASOURCE_CATALOG_IMPL)

    val datasources = new mutable.HashSet[String]()
    switches
      .filter(kv => "true".equalsIgnoreCase(appConf.get(kv._1)))
      .foreach(kv => datasources += kv._2)

    if (datasources.nonEmpty) {
      val c = Option(catalogImpl).getOrElse("hive").toLowerCase()
      if (!catalogSet.contains(c)) {
        throw new LivyProcessorException(
          s"Unknown $SPARK_SQL_DATASOURCE_CATALOG_IMPL=$catalogImpl")
      }

      // Overwrite to lowercase
      appConf.put(SPARK_SQL_DATASOURCE_CATALOG_IMPL, c)
      val sparkVersion = appConf.get(LivyConf.SPARK_FEATURE_VERSION)

      val jars = new ArrayBuffer[String]()
      datasources.foreach { datasource =>
        val dsJars = appConf.get(s"livy.rsc.spark.sql.catalog.$datasource.jars.v$sparkVersion")
        if (dsJars == null) {
          throw new LivyProcessorException(
            s"livy.rsc.spark.sql.catalog.$datasource.jars is empty.")
        }
        jars += dsJars
      }
      Option(appConf.get(SPARK_JARS)).filter(_.nonEmpty).foreach(jars += _)
      appConf.put(SPARK_JARS, jars.map(_.trim).mkString(","))
    }
  }
}
