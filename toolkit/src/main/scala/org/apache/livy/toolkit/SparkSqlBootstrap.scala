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
import java.util

import scala.collection.JavaConverters._

import com.opencsv.{CSVWriterBuilder, ICSVParser, ICSVWriter}
import org.apache.commons.cli.MissingArgumentException
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSqlBootstrap {
  @throws[IOException]
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      throw new MissingArgumentException("The first parameter sqlFileName is missing.")
    }
    val sqlFileName: String = args(0)
    val spark: SparkSession = SparkSession.builder.enableHiveSupport.getOrCreate()
    // https://jira.shopee.io/browse/SPDI-8214
    val csvWriter: ICSVWriter = new CSVWriterBuilder(new PrintWriter(System.out))
      .withSeparator(ICSVParser.DEFAULT_SEPARATOR)
      .withQuoteChar(ICSVParser.DEFAULT_QUOTE_CHARACTER)
      .withEscapeChar(ICSVParser.DEFAULT_QUOTE_CHARACTER)
      .withLineEnd(ICSVWriter.DEFAULT_LINE_END)
      .build
    val sdiSparkSqlExecutor: SparkSqlBootstrap =
      new SparkSqlBootstrap(spark, csvWriter)
    sdiSparkSqlExecutor.executeSqlFile(sqlFileName)
    sdiSparkSqlExecutor.close()
  }
}

class SparkSqlBootstrap(spark: SparkSession, csvWriter: ICSVWriter) {

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
    csvWriter.writeNext(fieldNames)
    val outputLimit = spark.sparkContext.getConf
      .getInt("spark.livy.output.limit.count", 0)
    val rows: util.List[Row] = if (outputLimit > 0) {
      dataset.takeAsList(outputLimit)
    } else {
      dataset.collectAsList()
    }
    rows.asScala.foreach { row =>
      val size: Int = row.size
      val array: Array[String] = new Array[String](size)
      for (i <- 0 until size) {
        val col: Any = row.get(i)
        array(i) = if (col == null) null else col.toString
      }
      csvWriter.writeNext(array)
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
        buf.append(line)
        if (isSqlEnd(line)) {
          sqlList.add(buf.toString)
          buf.setLength(0)
        }
        line = reader.readLine()
      }
    } finally if (reader != null) reader.close()
    if (buf.nonEmpty) {
      sqlList.add(buf.toString)
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
