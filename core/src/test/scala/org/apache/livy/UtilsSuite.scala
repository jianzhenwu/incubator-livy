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

import java.util

import org.scalatest.FunSuite

class UtilsSuite extends FunSuite with LivyBaseUnitTestSuite {

  test("should split sql with comment which start with --") {
    val sqlBuf = "-- show databases;\n" +
      "show databases;\n" +
      "-- show tables;\n" +
      "show tables;"
    val sql: util.List[String] = Utils.splitSemiColon(sqlBuf)
    assert(sql.size() == 2)
    assert(sql.get(0).equals("-- show databases;\nshow databases"))
    assert(sql.get(1).equals("\n-- show tables;\nshow tables"))
  }

  test("should split sql with block comment") {
    val sqlBuf = "/*show databases\n" +
      "*/\n" +
      "show databases;\n" +
      "/*show tables*/\n" +
      "show tables;"
    val sql = Utils.splitSemiColon(sqlBuf)
    assert(sql.size() == 2)
    assert(sql.get(0).equals("/*show databases\n" +
      "*/\n" +
      "show databases"))
    assert(sql.get(1).equals("\n/*show tables*/\n" +
      "show tables"))
  }

  test("should filter last comment line") {
    val sqlBuf = "select 1 as a,\n " +
      "-- 2\n" +
      "3 as b;\n" +
      "/*\n4\n*/"
    val sql = Utils.splitSemiColon(sqlBuf)
    assert(sql.size() == 1)
    assert(sql.get(0).equals("select 1 as a,\n " +
    "-- 2\n" +
      "3 as b"))

    val sqlBuf1 = "select 1 as a,\n " +
      "-- 2\n" +
      "3 as b;\n" +
      "--4\n"
    val sql1 = Utils.splitSemiColon(sqlBuf1)
    assert(sql1.size() == 1)
    assert(sql1.get(0).equals("select 1 as a,\n " +
      "-- 2\n" +
      "3 as b"))
  }

  test("should return empty when simple sql comment line") {
    val sqlBuf = "-- show databases;\n"
    val sql = Utils.splitSemiColon(sqlBuf)
    assert(sql.size() == 0)

    val sqlBuf1 = "/* show databases;*/\n"
    val sql1 = Utils.splitSemiColon(sqlBuf1)
    assert(sql1.size() == 0)
  }

  test("should return complete sql when comment end with semicolon") {
    val sqlBuf = "select 1, -- abc;\n" +
      "2"
    val sql = Utils.splitSemiColon(sqlBuf)
    assert(sql.size() == 1)
    assert(sql.get(0).equals("select 1, -- abc;\n2"))
  }

}
