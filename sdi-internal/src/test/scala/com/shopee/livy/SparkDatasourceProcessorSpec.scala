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

import com.shopee.livy.SparkDatasourceProcessor._
import com.shopee.livy.SparkDatasourceProcessorSpec._
import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import org.apache.livy.ApplicationEnvContext
import org.apache.livy.ApplicationEnvProcessor.SPARK_AUX_JAR

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

  @Deprecated
  val SPARK_SQL_CATALOG_HBASE_ENABLED = "spark.sql.catalog.hbase.enabled"
  val SPARK_LIVY_SQL_CATALOG_HBASE_ENABLED = "spark.livy.sql.catalog.hbase.enabled"

  @Deprecated
  val SPARK_SQL_CATALOG_JDBC_ENABLED = "spark.sql.catalog.jdbc.enabled"
  val SPARK_LIVY_SQL_CATALOG_JDBC_ENABLED = "spark.livy.sql.catalog.jdbc.enabled"
  @Deprecated
  val SPARK_SQL_CATALOG_KAFKA_ENABLED = "spark.sql.catalog.kafka.enabled"
  val SPARK_LIVY_SQL_CATALOG_KAFKA_ENABLED = "spark.livy.sql.catalog.kafka.enabled"
  @Deprecated
  val SPARK_SQL_CATALOG_TFRECORD_ENABLED = "spark.sql.catalog.tfrecord.enabled"
  val SPARK_LIVY_SQL_CATALOG_TFRECORD_ENABLED = "spark.livy.sql.catalog.tfrecord.enabled"

  val SPARK_SQL_CATALOG_HBASE_JARS = "livy.rsc.spark.sql.catalog.hbase.jars"
  val SPARK_SQL_CATALOG_JDBC_JARS = "livy.rsc.spark.sql.catalog.jdbc.jars"
  val SPARK_SQL_CATALOG_KAFKA_JARS = "livy.rsc.spark.sql.catalog.kafka.jars"
  val SPARK_SQL_CATALOG_TFRECORD_JARS = "livy.rsc.spark.sql.catalog.tfrecord.jars"
}

class SparkDatasourceProcessorSpec extends ScalatraSuite
  with FunSpecLike {

  private var processor: SparkDatasourceProcessor = _
  private var baseAppConf: mutable.Map[String, String] = _

  override def beforeAll(): Unit = {
    processor = new SparkDatasourceProcessor()
    baseAppConf = mutable.Map[String, String](
      SPARK_SQL_CATALOG_HBASE_JARS -> HBASE_JARS,
      SPARK_SQL_CATALOG_JDBC_JARS -> JDBC_JARS,
      SPARK_SQL_CATALOG_KAFKA_JARS -> KAFKA_JARS,
      SPARK_SQL_CATALOG_TFRECORD_JARS -> TFRECORD_JARS
    )
  }

  describe("SparkDatasourceProcessorSpec") {

    it("should use hbase datasource in hive when enabled") {

      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_HBASE_ENABLED -> "true",
        SPARK_SQL_DATASOURCE_CATALOG_IMPL -> "hive"
      ) ++ baseAppConf

      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_AUX_JAR) should include(HBASE_JARS)
      appConf(SPARK_AUX_JAR) should not include SPACE

    }

    it("should use jdbc datasource in hive when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_JDBC_ENABLED -> "true",
        SPARK_SQL_DATASOURCE_CATALOG_IMPL -> "hive"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_AUX_JAR) should include(JDBC_JARS)
    }

    it("should use kafka datasource in hive when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_KAFKA_ENABLED -> "true",
        SPARK_SQL_DATASOURCE_CATALOG_IMPL -> "hive"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_AUX_JAR) should include(KAFKA_JARS)
      appConf(SPARK_AUX_JAR) should not include SPACE
    }

    it("should use TFRecord datasource in hive when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_TFRECORD_ENABLED -> "true",
        SPARK_SQL_DATASOURCE_CATALOG_IMPL -> "hive"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_AUX_JAR) should include(TFRECORD_JARS)
      appConf(SPARK_AUX_JAR) should not include SPACE
    }

    it("should use hbase datasource in memory when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_HBASE_ENABLED -> "true",
        SPARK_SQL_DATASOURCE_CATALOG_IMPL -> "in-memory"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_AUX_JAR) should include(HBASE_JARS)
      appConf(SPARK_AUX_JAR) should not include SPACE
    }

    it("should use jdbc datasource in memory when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_JDBC_ENABLED -> "true",
        SPARK_SQL_DATASOURCE_CATALOG_IMPL -> "in-memory"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_AUX_JAR) should include(JDBC_JARS)
      appConf(SPARK_AUX_JAR) should not include SPACE
    }

    it("should use kafka datasource in memory when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_KAFKA_ENABLED -> "true",
        SPARK_SQL_DATASOURCE_CATALOG_IMPL -> "in-memory"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_AUX_JAR) should include(KAFKA_JARS)
      appConf(SPARK_AUX_JAR) should not include SPACE
    }

    it("should use TFRecord datasource in memory when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_TFRECORD_ENABLED -> "true",
        SPARK_SQL_DATASOURCE_CATALOG_IMPL -> "in-memory"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_AUX_JAR) should include(TFRECORD_JARS)
      appConf(SPARK_AUX_JAR) should not include SPACE
    }

    it("should use multiple datasources in hive when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_HBASE_ENABLED -> "true",
        SPARK_LIVY_SQL_CATALOG_JDBC_ENABLED -> "true",
        SPARK_SQL_DATASOURCE_CATALOG_IMPL -> "hive"
      ) ++ baseAppConf

      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_AUX_JAR) should include(HBASE_JARS)
      appConf(SPARK_AUX_JAR) should include(JDBC_JARS)
      appConf(SPARK_AUX_JAR) should not include SPACE
    }

    it("should use multiple datasources in memory when enabled") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_HBASE_ENABLED -> "true",
        SPARK_LIVY_SQL_CATALOG_JDBC_ENABLED -> "true",
        SPARK_SQL_DATASOURCE_CATALOG_IMPL -> "in-memory"
      ) ++ baseAppConf

      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)

      appConf(SPARK_AUX_JAR) should include(HBASE_JARS)
      appConf(SPARK_AUX_JAR) should include(JDBC_JARS)
    }

    it("should throw exception when using other catalog-impl") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_HBASE_ENABLED -> "true",
        SPARK_SQL_DATASOURCE_CATALOG_IMPL -> "other"
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      val e = the[ProcessorException] thrownBy processor.process(context)
      e.getMessage should be("Unknown spark.sql.datasource.catalog.impl=other")
    }

    it("should use default catalog-impl when it is null") {
      val appConf = mutable.Map[String, String](
        SPARK_LIVY_SQL_CATALOG_HBASE_ENABLED -> "true",
        SPARK_SQL_DATASOURCE_CATALOG_IMPL -> null
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)
      appConf(SPARK_AUX_JAR) should include(HBASE_JARS)
      appConf(SPARK_SQL_DATASOURCE_CATALOG_IMPL) should be ("hive")
    }

    it("should pass when there is not catalog enabled") {
      val appConf = mutable.Map[String, String](
      ) ++ baseAppConf
      val context = ApplicationEnvContext(Collections.emptyMap(), appConf.asJava)
      processor.process(context)
    }
  }
}
