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

package org.apache.livy

import java.io.{Closeable, File, InputStreamReader}
import java.net.URL
import java.nio.charset.StandardCharsets.UTF_8
import java.security.SecureRandom
import java.util
import java.util.Properties

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.TimeoutException
import scala.concurrent.duration.Duration

import org.apache.commons.codec.binary.Base64

object Utils {
  def getPropertiesFromFile(file: File): Map[String, String] = {
    loadProperties(file.toURI().toURL())
  }

  def loadProperties(url: URL): Map[String, String] = {
    val inReader = new InputStreamReader(url.openStream(), UTF_8)
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().asScala.map { k =>
        (k, properties.getProperty(k).trim())
      }.toMap
    } finally {
      inReader.close()
    }
  }

  /**
   * Checks if event has occurred during some time period. This performs an exponential backoff
   * to limit the poll calls.
   *
   * @param checkForEvent
   * @param atMost
   * @throws java.util.concurrent.TimeoutException
   * @throws java.lang.InterruptedException
   * @return
   */
  @throws(classOf[TimeoutException])
  @throws(classOf[InterruptedException])
  final def waitUntil(checkForEvent: () => Boolean, atMost: Duration): Unit = {
    val endTime = System.currentTimeMillis() + atMost.toMillis

    @tailrec
    def aux(count: Int): Unit = {
      if (!checkForEvent()) {
        val now = System.currentTimeMillis()

        if (now < endTime) {
          val sleepTime = Math.max(10 * (2 << (count - 1)), 1000)
          Thread.sleep(sleepTime)
          aux(count + 1)
        } else {
          throw new TimeoutException
        }
      }
    }

    aux(1)
  }

  /** Returns if the process is still running */
  def isProcessAlive(process: Process): Boolean = {
    try {
      process.exitValue()
      false
    } catch {
      case _: IllegalThreadStateException =>
        true
    }
  }

  def startDaemonThread(name: String)(f: => Unit): Thread = {
    val thread = new Thread(name) {
      override def run(): Unit = f
    }
    thread.setDaemon(true)
    thread.start()
    thread
  }

  def usingResource[A <: Closeable, B](resource: A)(f: A => B): B = {
    try {
      f(resource)
    } finally {
      resource.close()
    }
  }

  def createSecret(secretBitLength: Int): String = {
    val rnd = new SecureRandom()
    val secretBytes = new Array[Byte](secretBitLength / java.lang.Byte.SIZE)
    rnd.nextBytes(secretBytes)

    Base64.encodeBase64String(secretBytes)
  }

  def splitSemiColon(line: String): util.List[String] = {
    var insideSingleQuote = false
    var insideDoubleQuote = false
    var insideSimpleComment = false
    var bracketedCommentLevel = 0
    var escape = false
    var beginIndex = 0
    var leavingBracketedComment = false
    var isStatement = false
    val ret = new util.ArrayList[String]

    def insideBracketedComment: Boolean = bracketedCommentLevel > 0
    def insideComment: Boolean = insideSimpleComment || insideBracketedComment
    def statementInProgress(index: Int): Boolean = isStatement || (!insideComment &&
      index > beginIndex && !s"${line.charAt(index)}".trim.isEmpty)

    for (index <- 0 until line.length) {
      // Checks if we need to decrement a bracketed comment level; the last character '/' of
      // bracketed comments is still inside the comment, so `insideBracketedComment` must keep true
      // in the previous loop and we decrement the level here if needed.
      if (leavingBracketedComment) {
        bracketedCommentLevel -= 1
        leavingBracketedComment = false
      }

      if (line.charAt(index) == '\'' && !insideComment) {
        // take a look to see if it is escaped
        // See the comment above about SPARK-31595
        if (!escape && !insideDoubleQuote) {
          // flip the boolean variable
          insideSingleQuote = !insideSingleQuote
        }
      } else if (line.charAt(index) == '\"' && !insideComment) {
        // take a look to see if it is escaped
        // See the comment above about SPARK-31595
        if (!escape && !insideSingleQuote) {
          // flip the boolean variable
          insideDoubleQuote = !insideDoubleQuote
        }
      } else if (line.charAt(index) == '-') {
        val hasNext = index + 1 < line.length
        if (insideDoubleQuote || insideSingleQuote || insideComment) {
          // Ignores '-' in any case of quotes or comment.
          // Avoids to start a comment(--) within a quoted segment or already in a comment.
          // Sample query: select "quoted value --"
          //                                    ^^ avoids starting a comment if it's inside quotes.
        } else if (hasNext && line.charAt(index + 1) == '-') {
          // ignore quotes and ; in simple comment
          insideSimpleComment = true
        }
      } else if (line.charAt(index) == ';') {
        if (insideSingleQuote || insideDoubleQuote || insideComment) {
          // do not split
        } else {
          if (isStatement) {
            // split, do not include ; itself
            ret.add(line.substring(beginIndex, index))
          }
          beginIndex = index + 1
          isStatement = false
        }
      } else if (line.charAt(index) == '\n') {
        // with a new line the inline simple comment should end.
        if (!escape) {
          insideSimpleComment = false
        }
      } else if (line.charAt(index) == '/' && !insideSimpleComment) {
        val hasNext = index + 1 < line.length
        if (insideSingleQuote || insideDoubleQuote) {
          // Ignores '/' in any case of quotes
        } else if (insideBracketedComment && line.charAt(index - 1) == '*' ) {
          // Decrements `bracketedCommentLevel` at the beginning of the next loop
          leavingBracketedComment = true
        } else if (hasNext && line.charAt(index + 1) == '*') {
          bracketedCommentLevel += 1
        }
      }
      // set the escape
      if (escape) {
        escape = false
      } else if (line.charAt(index) == '\\') {
        escape = true
      }

      isStatement = statementInProgress(index)
    }
    // Check the last char is end of nested bracketed comment.
    val endOfBracketedComment = leavingBracketedComment && bracketedCommentLevel == 1
    // Spark SQL support simple comment and nested bracketed comment in query body.
    // But if Spark SQL receives a comment alone, it will throw parser exception.
    // In Spark SQL CLI, if there is a completed comment in the end of whole query,
    // since Spark SQL CLL use `;` to split the query, CLI will pass the comment
    // to the backend engine and throw exception. CLI should ignore this comment,
    // If there is an uncompleted statement or an uncompleted bracketed comment in the end,
    // CLI should also pass this part to the backend engine, which may throw an exception
    // with clear error message.
    if (!endOfBracketedComment && (isStatement || insideBracketedComment)) {
      ret.add(line.substring(beginIndex))
    }
    ret
  }
}
