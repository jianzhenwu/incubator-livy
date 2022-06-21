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

import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.text.{DateFormat, SimpleDateFormat}
import java.time.{Instant, LocalDate}
import java.util.{Locale, TimeZone}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator.primitiveTypes
import org.apache.spark.sql.catalyst.util.{DateTimeUtils => SparkDateTimeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
/**
 * Runs a query returning the result in Hive compatible form.
 */
object HiveResult {

  /**
   * Returns the result as a hive compatible sequence of strings.
   */
  def hiveResultStringForCsv(dataset: Seq[Row]): Seq[Seq[String]] = {
    dataset.map(hiveResultStringForCsv)
  }

  def hiveResultStringForCsv(row: Row): Seq[String] = {
    val columns: Seq[Any] = row.toSeq
    val types = row.schema.map(_.dataType)
    columns.zip(types).map(e => toHiveString(e, false))
  }

  /** Formats a datum (based on the given data type) and returns the string representation. */
  def toHiveString(a: (Any, DataType), nested: Boolean): String = a match {
    case (null, _) => if (nested) "null" else "NULL"

    case (b, BooleanType) => b.toString

    case (d: Date, DateType) =>
      DateTimeUtils.dateToString(SparkDateTimeUtils.fromJavaDate(d))

    case (ld: LocalDate, DateType) =>
      DateTimeUtils.dateToString(DateTimeUtils.localDateToDays(ld))

    case (t: Timestamp, TimestampType) =>
      DateTimeUtils.timestampToString(
        SparkDateTimeUtils.fromJavaTimestamp(t),
        SparkDateTimeUtils.TimeZoneUTC)

    case (i: Instant, TimestampType) =>
      DateTimeUtils.timestampToString(
        SparkDateTimeUtils.instantToMicros(i),
        SparkDateTimeUtils.TimeZoneUTC)

    case (bin: Array[Byte], BinaryType) => new String(bin, StandardCharsets.UTF_8)

    case (decimal: java.math.BigDecimal, DecimalType()) => decimal.toPlainString

    case (n, _: NumericType) => n.toString

    case (s: String, StringType) => if (nested) "\"" + s + "\"" else s

    case (interval: CalendarInterval, CalendarIntervalType) => interval.toString

    case (seq: scala.collection.Seq[_], ArrayType(typ, _)) =>
      seq.map(v => (v, typ)).map(e => toHiveString(e, true)).mkString("[", ",", "]")

    case (m: Map[_, _], MapType(kType, vType, _)) =>
      m.map { case (key, value) =>
        toHiveString((key, kType), true) + ":" +
          toHiveString((value, vType), true)
      }.toSeq.sorted.mkString("{", ",", "}")

    case (struct: Row, StructType(fields)) =>
      struct.toSeq.zip(fields).map { case (v, t) =>
        s""""${t.name}":${toHiveString((v, t.dataType), true)}"""
      }.mkString("{", ",", "}")

    case (other, tpe) if primitiveTypes.contains(tpe) => other.toString
  }
}

object DateTimeUtils {
  // we use Int and Long internally to represent [[DateType]] and [[TimestampType]]
  type SQLDate = Int
  type SQLTimestamp = Long

  // `SimpleDateFormat` is not thread-safe.
  private val threadLocalDateFormat = new ThreadLocal[DateFormat] {
    override def initialValue(): SimpleDateFormat = {
      new SimpleDateFormat("yyyy-MM-dd", Locale.US)
    }
  }

  def getThreadLocalDateFormat(timeZone: TimeZone): DateFormat = {
    val sdf = threadLocalDateFormat.get()
    sdf.setTimeZone(timeZone)
    sdf
  }

  // `SimpleDateFormat` is not thread-safe.
  private val threadLocalTimestampFormat = new ThreadLocal[DateFormat] {
    override def initialValue(): SimpleDateFormat = {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    }
  }

  def getThreadLocalTimestampFormat(timeZone: TimeZone): DateFormat = {
    val sdf = threadLocalTimestampFormat.get()
    sdf.setTimeZone(timeZone)
    sdf
  }

  /**
   * Converts the local date to the number of days since 1970-01-01.
   */
  def localDateToDays(localDate: LocalDate): Int = Math.toIntExact(localDate.toEpochDay)

  def dateToString(days: SQLDate): String =
    getThreadLocalDateFormat(TimeZone.getDefault()).format(SparkDateTimeUtils.toJavaDate(days))

  /**
   * Converts Timestamp to string according to Hive TimestampWritable convention.
   */
  def timestampToString(us: SQLTimestamp): String = {
    timestampToString(us, TimeZone.getDefault())
  }

  /**
   *  Converts Timestamp to string according to Hive TimestampWritable convention.
   */
  def timestampToString(us: SQLTimestamp, timeZone: TimeZone): String = {
    val ts = SparkDateTimeUtils.toJavaTimestamp(us)
    val timestampString = ts.toString
    val timestampFormat = getThreadLocalTimestampFormat(timeZone)
    val formatted = timestampFormat.format(ts)

    if (timestampString.length > 19 && timestampString.substring(19) != ".0") {
      formatted + timestampString.substring(19)
    } else {
      formatted
    }
  }
}
