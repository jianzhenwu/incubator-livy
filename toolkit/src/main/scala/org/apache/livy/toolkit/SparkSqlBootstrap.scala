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

package org.apache.livy.toolkit

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util

import scala.collection.JavaConverters._
import scala.util.Try

import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import org.apache.commons.cli.MissingArgumentException
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import org.apache.livy.Utils

object SparkSqlBootstrap {
  @throws[IOException]
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      throw new MissingArgumentException("The first parameter sqlFileName is missing.")
    }
    val spark: SparkSession = SparkSession.builder.enableHiveSupport.getOrCreate()
    val sparkConf: SparkConf = spark.sparkContext.getConf
    val sqlFileName: String = args(0)
    val outputPath: Option[String] = if (sparkConf.contains("spark.livy.sql.bootstrap.output")) {
      Option(sparkConf.get("spark.livy.sql.bootstrap.output"))
    } else {
      None
    }
    val overwrite: Boolean = if (sparkConf.contains("spark.livy.sql.bootstrap.output.overwrite")) {
      Try (sparkConf.get("spark.livy.sql.bootstrap.output.overwrite").toBoolean).getOrElse(false)
    } else {
      false
    }
    // https://jira.shopee.io/browse/SPDI-8214
    val writerSettings = new CsvWriterSettings()
    val format = writerSettings.getFormat
    format.setDelimiter(',')
    format.setQuote('"')
    format.setQuoteEscape('"')
    format.setComment('\u0000')
    writerSettings.setIgnoreLeadingWhitespaces(false)
    writerSettings.setIgnoreTrailingWhitespaces(false)
    writerSettings.setNullValue("NULL")
    writerSettings.setEmptyValue("")
    writerSettings.setSkipEmptyLines(true)
    writerSettings.setQuoteAllFields(true)
    writerSettings.setQuoteEscapingEnabled(true)
    val writer =
      outputPath.fold(new OutputStreamWriter(System.out, StandardCharsets.UTF_8)) { file =>
        val fs = FileSystem.get(new URI(file), spark.sparkContext.hadoopConfiguration)
        if (fs.exists(new Path(file)) && !overwrite) {
          throw new IllegalArgumentException(s"Output file $file is existed.")
        }
        new OutputStreamWriter(fs.create(new Path(file)), StandardCharsets.UTF_8)
      }
    val csvWriter = new CsvWriter(writer, writerSettings)

    val sparkSqlBootstrap: SparkSqlBootstrap =
      new SparkSqlBootstrap(spark, csvWriter)
    sparkSqlBootstrap.executeSqlFile(sqlFileName)
    sparkSqlBootstrap.close()
  }
}

class SparkSqlBootstrap(spark: SparkSession, csvWriter: CsvWriter) {

  @throws[IOException]
  private def executeSqlFile(fileName: String): Unit = {
    val sqlList: util.List[String] = readSqlFile(fileName)

    sqlList.asScala.foreach { e =>
      var sql: String = e
      if (isSqlEnd(sql)) sql = sql.substring(0, sql.length - 1)
      val dataset: DataFrame = spark.sql(sql)
      outputResult(dataset)
    }
  }

  private def isSqlEnd(sql: String): Boolean = sql.endsWith(";") && !sql.endsWith("\\;")

  @throws[IOException]
  private def outputResult(dataset: DataFrame): Unit = {
    // header
    val fieldNames: Array[String] = dataset.schema.fieldNames
    if (fieldNames.length > 0) {
      csvWriter.writeRow(fieldNames: _*)
    }

    val outputLimit = spark.sparkContext.getConf
      .getInt("spark.livy.output.limit.count", 0)
    val rows: util.List[Row] = if (outputLimit > 0) {
      dataset.takeAsList(outputLimit)
    } else {
      dataset.collectAsList()
    }
    rows.asScala.filter(_.size > 0).foreach { row =>
      csvWriter.writeRow(row.toSeq.map(_.asInstanceOf[Object]): _*)
    }

    csvWriter.flush()
  }

  @throws[IOException]
  private def readSqlFile(fileName: String): util.List[String] = {

    val sqlList: util.List[String] = new util.ArrayList[String]
    val buf: StringBuilder = new StringBuilder
    var line: String = null
    var reader: BufferedReader = null
    try {
      var inputStream: InputStream = this.getClass.getClassLoader
        .getResourceAsStream(new File(fileName).getName)
      // For unit testing
      if (inputStream == null) {
        inputStream = new FileInputStream(new File(fileName))
      }
      reader = new BufferedReader(new InputStreamReader(inputStream))
      line = reader.readLine
      while (line != null) {
        buf.append(line).append("\n")
        line = reader.readLine()
      }
    } finally if (reader != null) reader.close()
    if (buf.nonEmpty) {
      sqlList.addAll(Utils.splitSemiColon(buf.toString))
      buf.setLength(0)
    }
    sqlList
  }

  @throws[IOException]
  private def close(): Unit = {
    if (csvWriter != null) csvWriter.close()
    if (spark != null) spark.close
  }
}
