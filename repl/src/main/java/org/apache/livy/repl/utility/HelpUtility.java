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

package org.apache.livy.repl.utility;

import scala.Option;
import scala.util.Properties;

import org.apache.spark.sql.SparkSession;

public class HelpUtility implements Utility {
  @Override
  public String name() {
    return "help";
  }

  public String version() {
    StringBuilder sb = new StringBuilder();
    sb.append("Java Version: ").append(System.getProperty("java.version")).append("\n");
    sb.append("Scala Version: ").append(Properties.versionString()).append("\n");;

    Option<SparkSession> sso = SparkSession.getActiveSession();
    if(sso.isDefined()) {
      SparkSession ss = sso.get();
      sb.append("Spark Version: ").append(ss.version()).append("\n");;
    } else {
      sb.append("SparkSession is not available");
    }
    return sb.toString();
  }

  public String echo(String input) {
    return input;
  }

  public String pi(int decimal) {
    String format = "%." + decimal + "f";
    return String.format(format, Math.PI);
  }
}
