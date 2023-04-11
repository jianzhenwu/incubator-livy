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

package org.apache.livy.repl.utility

import java.io.{ByteArrayOutputStream, OutputStream, PrintStream}
import java.lang.reflect.Field
import java.util

import scala.collection.JavaConverters._
import scala.util.matching.Regex

import org.apache.commons.lang.WordUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.shell.{Command, CommandFactory, FsCommand}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.tools.TableListing

import org.apache.livy.Logging
import org.apache.livy.Utils.usingResource

object FsUtility {
  private val USAGE_PREFIX = "%utils fs"
  private val COMMAND_USAGE_FIELD = "USAGE"
  private val MAX_LINE_WIDTH = 80

  def apply(): FsUtility = {
    new FsUtility()
  }
}

class FsUtility extends Utility with Logging {

  import FsUtility._

  private var commandFactory: CommandFactory = _

  private val configuration = new Configuration()

  init()

  private def init(): Unit = {
    configuration.setQuietMode(true)
    UserGroupInformation.setConfiguration(configuration)
    commandFactory = new CommandFactory(configuration)
    commandFactory.addObject(new Help(), "-help")
    commandFactory.addObject(new Usage(), "-usage")

    registerCommands(commandFactory)
  }

  private def registerCommands(factory: CommandFactory): Unit = {
    factory.registerCommands(classOf[FsCommand])
  }

  override def name: String = "fs"

  def help(input: String): String = {
    run("help", input)
  }

  def usage(input: String): String = {
    run("usage", input)
  }

  def ls(input: String): String = {
    run("ls", input)
  }

  def cp(input: String): String = {
    run("cp", input)
  }

  def count(input: String): String = {
    run("count", input)
  }

  def cat(input: String): String = {
    run("cat", input)
  }

  def mkdir(input: String): String = {
    run("mkdir", input)
  }

  def put(input: String): String = {
    run("put", input)
  }

  def get(input: String): String = {
    run("get", input)
  }

  def mv(input: String): String = {
    run("mv", input)
  }

  def rm(input: String): String = {
    run("rm", input)
  }

  def rmr(input: String): String = {
    run("rmr", input)
  }

  def du(input: String): String = {
    run("du", input)
  }

  private def run(cmdName: String, options: String): String = {
    usingResource(List(new ByteArrayOutputStream(), new ByteArrayOutputStream())) { streams =>
      val out = streams.head
      val err = streams.last
      runInternal(cmdName, options, out, err) match {
        case 0 => out.toString
        case _ =>
          val msg = s"${err.toString}\n${out.toString}"
          throw ExecuteCommandException(msg)
      }
    }
  }

  private def runInternal(
      cmdName: String,
      options: String,
      out: OutputStream,
      err: OutputStream): Int = {
    val argv = if (options.isEmpty) {
      Array.empty[String]
    } else {
      options.split(" ")
    }

    var instance: Command = null
    var exitCode = -1

    try {
      instance = getCommandInstance(cmdName)
      // Transfer output stream (System.out/System.err) to destination stream.
      instance.out = new PrintStream(out, true, "UTF-8")
      instance.err = new PrintStream(err, true, "UTF-8")
      exitCode = instance.run(argv: _*)
    } catch {
      case ie: IllegalArgumentException =>
        error(s"Failed to execute $cmdName $options", ie)
        if (ie.getMessage == null) {
          displayError(err, "Null exception message")
        } else {
          displayError(err, ie.getLocalizedMessage)
        }
        if (instance != null) {
          printInstanceUsage(instance);
        }

      case e: Exception =>
        // instance.run catches IOE, so something is REALLY wrong if here.
        error(s"Failed to execute $cmdName $options", e)
        displayError(err,
          s"""$cmdName: Fatal internal error
             |$e""".stripMargin);
    }
    exitCode
  }

  private def getCommandInstance(cmdName: String) = {
    val instance = commandFactory.getInstance(s"-$cmdName")
    if (instance == null) {
      throw new UnknownCommandException(cmdName)
    }
    instance
  }

  private def getUsagePrefix = s"Usage: $USAGE_PREFIX"

  private def displayError(err: OutputStream, message: String): Unit = {
    err.write(s"$message".getBytes)
  }

  private def printUsage(out: OutputStream, cmd: Option[String]): Unit = {
    printInfo(out, cmd, false)
  }

  private def printHelp(out: OutputStream, cmd: Option[String]): Unit = {
    printInfo(out, cmd, true)
  }

  private def printInfo(out: OutputStream, cmd: Option[String], showHelp: Boolean): Unit = {
    cmd match {
      case Some(c) =>
        val instance = getCommandInstance(c)
        instance.out = new PrintStream(out, true, "UTF-8")
        if (showHelp) {
          printInstanceHelp(instance)
        } else {
          printInstanceUsage(instance)
        }

      case _ =>
        commandFactory.getNames.foreach(name => {
          val instance = commandFactory.getInstance(name)
          // Transfer output stream of command instance to destination stream.
          instance.out = new PrintStream(out, true, "UTF-8")

          // scalastyle:off println
          // display list of short usages.
          instance.out.println(getInstanceUsageWithPrefix(instance, getUsagePrefix))
          // display long descriptions for each command.
          if (showHelp) {
            instance.out.println()
            printInstanceHelp(instance)
          }
          instance.out.println()
          // scalastyle:on println
        })
    }
  }

  private def getCommandField(instance: Command, field: String): String = {
    try {
      val f: Field = instance.getClass.getDeclaredField(field)
      f.setAccessible(true)
      f.get(instance).toString
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"failed to get ${this.getClass.getSimpleName}.$field", e)
    }
  }

  private def getInstanceUsageWithPrefix(instance: Command, prefix: String): String = {
    if (instance.isDeprecated) {
      s"$prefix.${instance.getName}"
    } else {
      s"$prefix.${instance.getName}(${getCommandField(instance, COMMAND_USAGE_FIELD)})"
    }
  }

  private def printInstanceUsage(instance: Command): Unit = {
    // scalastyle:off println
    instance.out.println(getInstanceUsageWithPrefix(instance, getUsagePrefix))
    // scalastyle:on println
  }

  private def printInstanceHelp(instance: Command): Unit = {
    // scalastyle:off println
    instance.out.println(s"${getInstanceUsageWithPrefix(instance, USAGE_PREFIX)} :")

    var listing: TableListing = null
    val prefix: String = "  "

    instance.getDescription.split("\n").foreach(line => {
      val pattern = new Regex("^[ \t]*[-<].*$")
      line match {
        case pattern(line) =>
          val segments: Array[String] = line.split(":")
          if (segments.length == 2) {
            if (listing == null) {
              listing = createOptionTableListing()
            }
            listing.addRow(segments(0).trim, segments(1).trim)
          }
        case _ =>
          // Normal literal description.
          if (listing != null) {
            listing.toString.split("\n").foreach { listingLine =>
              instance.out.println(s"$prefix$listingLine")
            }
            listing = null
          }
          WordUtils.wrap(line, MAX_LINE_WIDTH, "\n", true)
            .split("\n")
            .foreach(descLine => instance.out.println(s"$prefix$descLine"))
      }
    })

    if (listing != null) {
      listing.toString.split("\n").foreach { listingLine =>
        instance.out.println(s"$prefix$listingLine")
      }
    }
    // scalastyle:on println
  }

  /**
   * Creates a two-row table, the first row is for the command line option,
   * the second row is for the option description.
   */
  private def createOptionTableListing(): TableListing =
    new TableListing.Builder()
      .addField("")
      .addField("", true)
      .wrapWidth(MAX_LINE_WIDTH)
      .build

  /**
   * Display help for commands with their short usage and long description.
   */
  class Usage extends FsCommand {
    val NAME = "usage"
    val USAGE = "[cmd ...]"
    val DESCRIPTION: String = "Displays the usage for given command or " +
      "all commands if none is specified."

    override protected def processRawArguments(args: util.LinkedList[String]): Unit = {
      if (args.isEmpty) {
        printUsage(out, None)
      } else {
        args.asScala.foreach(arg => printUsage(out, Some(arg)))
      }
    }
  }

  /**
   * Displays short usage of commands sans the long description
   */
  class Help extends FsCommand {
    val NAME = "help"
    val USAGE = "[cmd ...]"
    val DESCRIPTION: String = "Displays help for given command or " +
      "all commands if none is specified."

    override protected def processRawArguments(args: util.LinkedList[String]): Unit = {
      if (args.isEmpty) {
        printHelp(out, None)
      } else {
        args.asScala.foreach(arg => printHelp(out, Some(arg)))
      }
    }
  }

  /**
   * The default ctor signals that the command being executed does not exist,
   * while other ctor signals that a specific command does not exist.  The
   * latter is used by commands that process other commands, ex. usage/help
   */
  class UnknownCommandException(val cmd: String) extends IllegalArgumentException {
    override def getMessage: String = {
      s"${if (cmd != null) s"$USAGE_PREFIX.$cmd: " else ""} Unknown command"
    }
  }
}

case class ExecuteCommandException(message: String) extends RuntimeException(message)
