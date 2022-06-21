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

import java.math.BigInteger
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, FunSuite}

class HiveResultSuite extends FunSuite with BeforeAndAfter {

  test("string formatting in hive result") {
    val strs = List("1", "2", "3")
    val oschema = new StructType().add("a", StringType)

    strs.foreach(str => {
      val row = new GenericRowWithSchema(List(str).toArray, oschema)
      val result = HiveResult.hiveResultStringForCsv(row)
      assert(result.head.equals(str))
    })
  }

  test("date formatting in hive result") {
    val dates = List(
      Date.valueOf("2018-12-28"),
      Date.valueOf("1582-10-03"),
      Date.valueOf("1582-10-04"),
      Date.valueOf("1582-10-15"))
    val oschema = new StructType().add("a", DateType)

    dates.foreach(date => {
      val row = new GenericRowWithSchema(List(date).toArray, oschema)
      val result = HiveResult.hiveResultStringForCsv(row)
      result.foreach(r => {
        assert(r == date.toString)
      })
    })

  }

  test("array formatting in hive result") {
    val arrays = List(Seq("1", "2"), Seq("2", "2"), Seq("3", "2"), Seq("4", "2"))
    val expected = Seq(("1", "2"), ("2", "2"), ("3", "2"), ("4", "2"))
    val oschema = new StructType().add("a", DataTypes.createArrayType(StringType))

    var index = 0
    arrays.foreach(array => {
      val row = new GenericRowWithSchema(List(array).toArray, oschema)
      val result = HiveResult.hiveResultStringForCsv(row)
      assert(result.head == s"""["${expected(index)._1}","${expected(index)._2}"]""")
      index += 1
    })
  }

  test("timestamp formatting in hive result") {
    val timestamps = List(
      Timestamp.valueOf("2018-12-28 01:02:03"),
      Timestamp.valueOf("1582-10-03 01:02:03"),
      Timestamp.valueOf("1582-10-04 01:02:03"),
      Timestamp.valueOf("1582-10-25 01:02:03"))
    val oschema = new StructType().add("a", TimestampType)

    var index = 0
    timestamps.foreach(timestamp => {
      val row = new GenericRowWithSchema(List(timestamp).toArray, oschema)
      val result = HiveResult.hiveResultStringForCsv(row)
      assert(result.head ==
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamps(index)))
      index += 1
    })
  }

  test("decimal formatting in hive result") {
    val oschema = new StructType().add("a", DataTypes.createDecimalType())

    Seq(2, 6, 18).foreach { scale =>
      val decimal = List(new java.math.BigDecimal(new BigInteger("1"), scale))
      val row = new GenericRowWithSchema(decimal.toArray, oschema)
      val result = HiveResult.hiveResultStringForCsv(row)
      assert(result.head.split("\\.").last.length == scale)
    }

    val row = new GenericRowWithSchema(
      List(java.math.BigDecimal.ZERO.setScale(8)).toArray, oschema)
    val result = HiveResult.hiveResultStringForCsv(row)
    assert(result.head == "0.00000000")
  }
}
