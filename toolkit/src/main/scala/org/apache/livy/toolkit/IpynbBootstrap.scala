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
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.google.gson.{Gson, GsonBuilder}
import org.apache.commons.cli.MissingArgumentException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.livy.Logging
import org.apache.livy.jupyter.JupyterUtil
import org.apache.livy.jupyter.nbformat._
import org.apache.livy.repl.{Session, TEXT_PLAIN}
import org.apache.livy.rsc.RSCConf
import org.apache.livy.rsc.driver.Statement

object IpynbBootstrap{
  private val MAGIC_TO_TYPE: Map[String, Option[String]] = Map(
    "%%sql" -> Option("sql"),
    "%%python" -> Option("python"),
    "%%scala" -> Option("scala"),
    "%%r" -> Option("r")
  )

  private val VALID_MAGIC: Set[String] = Set("%matplot plt")

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      throw new MissingArgumentException("Missing inputFileName or outputPath.")
    }

    val ipynbFileName: String = args(0)
    val outputPath: String = args(1)
    val spark: SparkSession = SparkSession.builder.enableHiveSupport.getOrCreate()
    val sparkContext = spark.sparkContext

    val ipynbBootstrap = new IpynbBootstrap(sparkContext.getConf, sparkContext.hadoopConfiguration)
    ipynbBootstrap.executeIpynb(ipynbFileName, outputPath)
    ipynbBootstrap.close()
    if (spark != null) spark.close()
  }
}

class IpynbBootstrap(sparkConf: SparkConf, hadoopConf: Configuration) extends Logging {
  import IpynbBootstrap._

  private val jupyterUtil: JupyterUtil = new JupyterUtil()
  private val rscConf = new RSCConf(null)
  rscConf.set(RSCConf.Entry.SESSION_KIND, "python")
  private val session: Session = new Session(rscConf, sparkConf)

  private lazy val mapper = new ObjectMapper()
    .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  private def startSession(): Unit = {
    Await.result(session.start(), Duration.Inf)
  }

  @throws[IOException]
  def executeIpynb(inputFileName: String, outputPath: String): Unit = {
    startSession()

    val inputNb: Nbformat = readNotebookFile(inputFileName)
    try {
      val codeCells = inputNb.getCells.asScala
        .filter(_.isInstanceOf[CodeCell]).map(_.asInstanceOf[CodeCell])
      continueExecute(codeCells)
    } finally {
      // write to the output file
      writeToFile(inputNb, outputPath)
    }
  }

  @throws[IOException]
  private def readNotebookFile(ipynbFileName: String): Nbformat = {
    val inputStream: InputStream = this.getClass.getClassLoader
      .getResourceAsStream(new File(ipynbFileName).getName)
    val reader: Reader = new InputStreamReader(inputStream)
    try {
      jupyterUtil.getNbformat(reader)
    } finally {
      if (reader != null) reader.close()
    }
  }

  // Visible for testing
  private[toolkit] def parseCode(sources: Array[String]): (Option[String], String) = {
    // Only support specific magic codes
    def isValidSource(line: Option[String]): Boolean = {
      line.forall( _.trim match {
        case l: String =>
          MAGIC_TO_TYPE.contains(l) || VALID_MAGIC.contains(l) || !l.startsWith("%")
      })
    }

    val validSources = sources.filter(line => isValidSource(Some(line)))

    if (validSources.isEmpty) {
      (None, "")
    } else {
      val codeType: Option[String] = MAGIC_TO_TYPE.getOrElse(validSources(0).trim, None)

      val validSourcesInLine = if (codeType.isDefined) {
        validSources.drop(1).mkString
      } else {
        validSources.mkString
      }

      (codeType, validSourcesInLine)
    }
  }

  // Visible for testing
  private[toolkit] def convertSqlMagicToPyspark(code: String): String = {
    val outputLimit = sparkConf.getInt("spark.livy.output.limit.count", 20)
    if (code.isEmpty) {
      code
    } else {
      s"""spark.sql(\"\"\" $code \"\"\").show($outputLimit)"""
    }
  }

  private def continueExecute(codeCells: Seq[CodeCell]): Unit = {
    if (codeCells.isEmpty) return
    val codeCell = codeCells(0)
    val sources = codeCell.getSource.asInstanceOf[util.ArrayList[String]].asScala.toArray

    // Parse the source code
    val (codeType, code) = parseCode(sources)

    def executeCode(codeType: Option[String], code: String): Map[String, Any] = {
      val statementId: Int = session.execute(code, codeType.getOrElse("python"))
      val output: Map[String, Any] = {
        // Wait and get the execution result
        val statement: Option[Statement] = session.statements.get(statementId)
        while (!statement.get.isFinished) {
          // Wait for the statement finished
          Thread.sleep(100)
        }
        mapper.readValue(statement.get.output, classOf[Map[String, Any]])
      }
      output
    }

    // Execute the code and get output
    val output: Map[String, Any] = codeType match {
      case Some("sql") =>
        executeCode(Some("python"), convertSqlMagicToPyspark(code))
      case t @ (Some(_) | None) =>
        executeCode(t, code)
    }

    // Write output to CodeCell
    val buffer = mutable.ArrayBuffer[Output]()
    output(Session.STATUS) match {
      case Session.OK =>
        val data = output(Session.DATA).asInstanceOf[Map[String, Any]]
        data.foreach {case (key, value) => {
          val out: Output = key match {
            case TEXT_PLAIN => new Stream(Stream.STDOUT, value)
            case _ => new DisplayData(Map(key -> value.asInstanceOf[Object]).asJava)
          }
          buffer.append(out)
        }}
      case Session.ERROR =>
        buffer.append(mapper.convertValue(output, classOf[Error]))
      case status =>
        throw new IllegalStateException(s"Unknown statement status: $status")
    }
    codeCell.setOutputs(buffer.toList.asJava)
    codeCell.setExecutionCount(output(Session.EXECUTION_COUNT).asInstanceOf[Integer])

    // Stop execution once we encounter the error
    if (output(Session.STATUS) == Session.ERROR) {
      val e = buffer(0).asInstanceOf[Error]
      val exception = new Exception(s"${e.getEname}: ${e.getEvalue}")
      error(s"Error in executing code: $code\n${e.getTraceback}")
      throw exception
    }

    continueExecute(codeCells.drop(1))
  }

  @throws[IOException]
  private def writeToFile(notebook: Nbformat, filePath: String): Unit = {
    val fs: FileSystem = FileSystem.get(URI.create(filePath), hadoopConf)
    val outputStream: OutputStream = fs.create(new Path(filePath))
    val writer: OutputStreamWriter = new OutputStreamWriter(outputStream)
    try {
      val gson: Gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create()
      gson.toJson(notebook, writer)
    } finally {
      if (writer != null) writer.close()
    }
  }

  @throws[IOException]
  def close(): Unit = {
    if (session != null) session.close()
  }
}
