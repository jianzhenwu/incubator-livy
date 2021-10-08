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

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain

object ScalaInterpreter {

  val Success: String = "Success"
  val Error: String = "Error"
  val Incomplete: String = "Incomplete"
  private val settings = new Settings
  settings.processArgumentString("-deprecation -feature -Xfatal-warnings -Xlint")
  settings.usejavacp.value = true

  private val intp = new IMain(settings)

  def parse(code: String): String = {
    intp.parse(code) match {
      case intp.parse.Incomplete(_) => Incomplete
      case intp.parse.Error(_) => Error
      case intp.parse.Success(_) => Success
    }
  }
}
