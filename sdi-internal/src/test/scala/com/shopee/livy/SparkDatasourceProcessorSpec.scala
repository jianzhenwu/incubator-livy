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

import java.util.Collections

import scala.collection.JavaConverters.mutableMapAsJavaMapConverter
import scala.collection.mutable

import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import org.apache.livy.{ApplicationEnvContext, LivyConf}
import org.apache.livy.ApplicationEnvProcessor.SPARK_JARS
import org.apache.livy.utils.LivyProcessorException

object SparkDatasourceProcessorSpec {
  val SPACE = " "
  val HBASE_JARS: String = "hbase-catalog-1.0-SNAPSHOT.jar," +
    "catalog-common-1.0-SNAPSHOT.jar"
  val JDBC_JARS: String = "jdbc-catalog-1.0-SNAPSHOT.jar," +
    "catalog-common-1.0-SNAPSHOT.jar"
  val KAFKA_JARS: String = "kafka-catalog-1.0-SNAPSHOT.jar," +
    "spark-sql-kafka-0-10_2.12-3.1.2.jar," +
    "spark-token-provider-kafka-0-10_2.12-3.1.2.jar," +
    "spark-streaming-kafka-0-10_2.12-3.1.2.jar," +
    "catalog-common-1.0-SNAPSHOT.jar," +
    "kafka-clients-2.6.0.jar," +
    "commons-pool2-2.6.2.jar"
  val TFRECORD_JARS: String = "tfrecord-catalog-1.0-SNAPSHOT.jar," +
    "catalog-common-1.0-SNAPSHOT.jar"
  val CLICKHOUSE_JARS: String = "clickhouse-catalog-1.0-SNAPSHOT.jar," +
    "catalog-common-1.0-SNAPSHOT.jar"

  val SPARK_LIVY_SQL_CATALOG_HBASE_ENABLED = "spark.livy.sql.catalog.hbase.enabled"
  val SPARK_LIVY_SQL_CATALOG_JDBC_ENABLED = "spark.livy.sql.catalog.jdbc.enabled"
  val SPARK_LIVY_SQL_CATALOG_KAFKA_ENABLED = "spark.livy.sql.catalog.kafka.enabled"
  val SPARK_LIVY_SQL_CATALOG_TFRECORD_ENABLED = "spark.livy.sql.catalog.tfrecord.enabled"
  val SPARK_LIVY_SQL_CATALOG_CLICKHOUSE_ENABLED = "spark.livy.sql.catalog.clickhouse.enabled"

  val SPARK_SQL_CATALOG_HBASE_JARS = "livy.rsc.spark.sql.catalog.hbase.jars.v3.1"
  val SPARK_SQL_CATALOG_JDBC_JARS = "livy.rsc.spark.sql.catalog.jdbc.jars.v3.1"
  val SPARK_SQL_CATALOG_KAFKA_JARS = "livy.rsc.spark.sql.catalog.kafka.jars.v3.1"
  val SPARK_SQL_CATALOG_TFRECORD_JARS = "livy.rsc.spark.sql.catalog.tfrecord.jars.v3.1"
  val SPARK_SQL_CATALOG_CLICKHOUSE_JARS = "livy.rsc.spark.sql.catalog.clickhouse.jars.v3.1"

  val SPARK_SQL_CATALOG_HBASE_IMPL = "spark.sql.catalog.hbase.impl"
  val SPARK_SQL_CATALOG_JDBC_IMPL = "spark.sql.catalog.jdbc.impl"
  val SPARK_SQL_CATALOG_KAFKA_IMPL = "spark.sql.catalog.kafka.impl"
  val SPARK_SQL_CATALOG_TFRECORD_IMPL = "spark.sql.catalog.tfrecord.impl"
  val SPARK_SQL_CATALOG_CLICKHOUSE_IMPL = "spark.sql.catalog.clickhouse.impl"
}

class SparkDatasourceProcessorSpec extends ScalatraSuite
  with FunSpecLike {

  import SparkDatasourceProcessorSpec._

  private var processor: SparkDatasourceProcessor = _
  private var baseAppConf: mutable.Map[String, String] = _

  override def beforeAll(): Unit = {
    processor = new SparkDatasourceProcessor()
    baseAppConf = mutable.Map[String, String](
      SPARK_SQL_CATALOG_HBASE_JARS -> HBASE_JARS,
      SPARK_SQL_CATALOG_JDBC_JARS -> JDBC_JARS,
      SPARK_SQL_CATALOG_KAFKA_JARS -> KAFKA_JARS,
      SPARK_SQL_CATALOG_TFRECORD_JARS -> TFRECORD_JARS,
      SPARK_SQL_CATALOG_CLICKHOUSE_JARS -> CLICKHOUSE_JARS,
      LivyConf.SPARK_FEATURE_VERSION -> "3.1"
    )
  }

  describe("SparkDatasourceProcessorSpec") {

    it("should use hbase datasource in hive when enabled") {

      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_HBASE_ENABLED -> "true",
        SPARK_SQL_CATALOG_HBASE_IMPL -> "hive"
      ) ++ baseAppConf

      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_JARS) should include(HBASE_JARS)
      appConf(SPARK_JARS) should not include SPACE

    }

    it("should use jdbc datasource in hive when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_JDBC_ENABLED -> "true",
        SPARK_SQL_CATALOG_JDBC_IMPL -> "hive"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_JARS) should include(JDBC_JARS)
    }

    it("should use kafka datasource in hive when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_KAFKA_ENABLED -> "true",
        SPARK_SQL_CATALOG_KAFKA_IMPL -> "hive"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_JARS) should include(KAFKA_JARS)
      appConf(SPARK_JARS) should not include SPACE
    }

    it("should use TFRecord datasource in hive when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_TFRECORD_ENABLED -> "true",
        SPARK_SQL_CATALOG_TFRECORD_IMPL -> "hive"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_JARS) should include(TFRECORD_JARS)
      appConf(SPARK_JARS) should not include SPACE
    }

    it("should thrown exception when use clickhouse datasource in hive") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_CLICKHOUSE_ENABLED -> "true",
        SPARK_SQL_CATALOG_CLICKHOUSE_IMPL -> "hive"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)

      assertThrows[LivyProcessorException] {
        processor.process(context)
      }
    }

    it("should use hbase datasource in memory when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_HBASE_ENABLED -> "true",
        SPARK_SQL_CATALOG_HBASE_IMPL -> "in-memory"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_JARS) should include(HBASE_JARS)
      appConf(SPARK_JARS) should not include SPACE
    }

    it("should use jdbc datasource in memory when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_JDBC_ENABLED -> "true",
        SPARK_SQL_CATALOG_JDBC_IMPL -> "in-memory"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_JARS) should include(JDBC_JARS)
      appConf(SPARK_JARS) should not include SPACE
    }

    it("should use kafka datasource in memory when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_KAFKA_ENABLED -> "true",
        SPARK_SQL_CATALOG_KAFKA_IMPL -> "in-memory"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_JARS) should include(KAFKA_JARS)
      appConf(SPARK_JARS) should not include SPACE
    }

    it("should use TFRecord datasource in memory when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_TFRECORD_ENABLED -> "true",
        SPARK_SQL_CATALOG_TFRECORD_IMPL -> "in-memory"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_JARS) should include(TFRECORD_JARS)
      appConf(SPARK_JARS) should not include SPACE
    }

    it("should use clickhouse datasource in memory when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_CLICKHOUSE_ENABLED -> "true",
        SPARK_SQL_CATALOG_CLICKHOUSE_IMPL -> "in-memory"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_JARS) should include(CLICKHOUSE_JARS)
      appConf(SPARK_JARS) should not include SPACE
    }

    it("should use multiple datasources in hive when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_HBASE_ENABLED -> "true",
        SPARK_LIVY_SQL_CATALOG_JDBC_ENABLED -> "true",
        SPARK_SQL_CATALOG_HBASE_IMPL -> "hive",
        SPARK_SQL_CATALOG_JDBC_IMPL -> "hive"
      ) ++ baseAppConf

      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_JARS) should include(HBASE_JARS)
      appConf(SPARK_JARS) should include(JDBC_JARS)
      appConf(SPARK_JARS) should not include SPACE
    }

    it("should use multiple datasources in memory when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_HBASE_ENABLED -> "true",
        SPARK_LIVY_SQL_CATALOG_JDBC_ENABLED -> "true",
        SPARK_SQL_CATALOG_HBASE_IMPL -> "in-memory",
        SPARK_SQL_CATALOG_JDBC_IMPL -> "in-memory"
      ) ++ baseAppConf

      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_JARS) should include(HBASE_JARS)
      appConf(SPARK_JARS) should include(JDBC_JARS)
    }

    it("should throw exception when using other catalog-impl") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_HBASE_ENABLED -> "true",
        SPARK_SQL_CATALOG_HBASE_IMPL -> "other"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      val e = the[LivyProcessorException] thrownBy processor.process(context)
      e.getMessage should be (
        "The value of spark.sql.catalog.hbase.impl should be " +
          "one of hive, in-memory, but was other")
    }

    it("should use default catalog-impl when it is null") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_HBASE_ENABLED -> "true",
        SPARK_LIVY_SQL_CATALOG_CLICKHOUSE_ENABLED -> "true",
        SPARK_SQL_CATALOG_HBASE_IMPL -> null
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)
      appConf(SPARK_JARS) should include(HBASE_JARS)
      appConf(SPARK_JARS) should include(CLICKHOUSE_JARS)
      appConf(SPARK_SQL_CATALOG_HBASE_IMPL) should be ("hive")
      appConf(SPARK_SQL_CATALOG_CLICKHOUSE_IMPL) should be ("in-memory")
    }

    it("should pass when there is not catalog enabled") {
      val appConf = mutable.Map[String, String](
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)
    }
  }
}
