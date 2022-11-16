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

package org.apache.livy.sessions

import java.net.URI

import org.scalatest.FunSuite

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}

class SessionSpec extends FunSuite with LivyBaseUnitTestSuite {

  test("use default fs in paths") {
    val conf = new LivyConf(false)
    conf.hadoopConf.set("fs.defaultFS", "dummy:///")

    val uris = Seq("http://example.com/foo", "hdfs:/bar", "/baz", "/foo#bar")
    val expected = Seq(uris(0), uris(1), "dummy:///baz", "dummy:///foo#bar")
    assert(Session.resolveURIs(uris, conf) === expected)

    intercept[IllegalArgumentException] {
      Session.resolveURI(new URI("relative_path"), conf)
    }
  }

  test("local fs whitelist") {
    val conf = new LivyConf(false)
    conf.set(LivyConf.LOCAL_FS_WHITELIST, "/allowed/,/also_allowed")

    Seq("/allowed/file", "/also_allowed/file").foreach { path =>
      assert(Session.resolveURI(new URI(path), conf) === new URI("file://" + path))
    }

    Seq("/not_allowed", "/allowed_not_really").foreach { path =>
      intercept[IllegalArgumentException] {
        Session.resolveURI(new URI(path), conf)
      }
    }
  }

  test("conf validation and preparation") {
    val conf = new LivyConf(false)
    conf.hadoopConf.set("fs.defaultFS", "dummy:///")
    conf.set(LivyConf.LOCAL_FS_WHITELIST, "/allowed")

    // Test baseline.
    assert(Session.prepareConf(Map(), Nil, Nil, Nil, Nil, conf) === Map("spark.master" -> "local"))

    // Test validations.
    intercept[IllegalArgumentException] {
      Session.prepareConf(Map("spark.do_not_set" -> "1"), Nil, Nil, Nil, Nil, conf)
    }
    conf.sparkFileLists.foreach { key =>
      intercept[IllegalArgumentException] {
        Session.prepareConf(Map(key -> "file:/not_allowed"), Nil, Nil, Nil, Nil, conf)
      }
    }
    intercept[IllegalArgumentException] {
      Session.prepareConf(Map(), Seq("file:/not_allowed"), Nil, Nil, Nil, conf)
    }
    intercept[IllegalArgumentException] {
      Session.prepareConf(Map(), Nil, Seq("file:/not_allowed"), Nil, Nil, conf)
    }
    intercept[IllegalArgumentException] {
      Session.prepareConf(Map(), Nil, Nil, Seq("file:/not_allowed"), Nil, conf)
    }
    intercept[IllegalArgumentException] {
      Session.prepareConf(Map(), Nil, Nil, Nil, Seq("file:/not_allowed"), conf)
    }

    // Test that file lists are merged and resolved.
    val base = "/file1.txt"
    val other = Seq("/file2.txt")
    val expected = Some(Seq("dummy://" + other(0), "dummy://" + base).mkString(","))

    val userLists = Seq(LivyConf.SPARK_JARS, LivyConf.SPARK_FILES, LivyConf.SPARK_ARCHIVES,
      LivyConf.SPARK_PY_FILES)
    val baseConf = userLists.map { key => (key -> base) }.toMap
    val result = Session.prepareConf(baseConf, other, other, other, other, conf)
    userLists.foreach { key => assert(result.get(key) === expected) }
  }

  test("should use preview spark when queue contains -dev") {
    val reqSparkVersion: Option[String] = Some("v3")
    val queue: Option[String] = Some("queue-dev")
    val livyConf: LivyConf = new LivyConf()
      .set(LivyConf.LIVY_SPARK_VERSIONS, "v3")
      .set("livy.server.spark-home.v3", "/usr/share/spark-3.1")
      .set("livy.server.spark-conf-dir.v3", "/etc/spark-3.1")
      .set("livy.server.spark-home.v3.preview", "/usr/share/spark-3.2")
      .set("livy.server.spark-conf-dir.v3.preview", "/etc/spark-3.2")
      .set(LivyConf.SPARK_LIVY_SPARK_PREVIEW_ENABLED, true)

    val sparkVersion = Session.reqSparkVersionOrPreview(reqSparkVersion, queue, livyConf)
    assert(sparkVersion.get == "v3.preview")
  }

  test("should work when no preview spark version") {
    val reqSparkVersion: Option[String] = Some("v3")
    val queue: Option[String] = Some("queue-dev")
    val livyConf: LivyConf = new LivyConf()
      .set(LivyConf.LIVY_SPARK_VERSIONS, "v3")
      .set("livy.server.spark-home.v3", "/usr/share/spark-3.1")
      .set("livy.server.spark-conf-dir.v3", "/etc/spark-3.1")
      .set(LivyConf.SPARK_LIVY_SPARK_PREVIEW_ENABLED, true)

    val sparkVersion = Session.reqSparkVersionOrPreview(reqSparkVersion, queue, livyConf)
    assert(sparkVersion.get == "v3")
  }

  test("should not use preview spark when disabled") {
    val reqSparkVersion: Option[String] = Some("v3")
    val queue: Option[String] = Some("queue-dev")
    val livyConf: LivyConf = new LivyConf()
      .set(LivyConf.LIVY_SPARK_VERSIONS, "v3")
      .set("livy.server.spark-home.v3", "/usr/share/spark-3.1")
      .set("livy.server.spark-conf-dir.v3", "/etc/spark-3.1")
      .set("livy.server.spark-home.v3.preview", "/usr/share/spark-3.2")
      .set("livy.server.spark-conf-dir.v3.preview", "/etc/spark-3.2")
      .set(LivyConf.SPARK_LIVY_SPARK_PREVIEW_ENABLED, false)

    val sparkVersion = Session.reqSparkVersionOrPreview(reqSparkVersion, queue, livyConf)
    assert(sparkVersion.get == "v3")
  }

  test("should not use preview spark when using prevent queue") {
    val reqSparkVersion: Option[String] = Some("v3")
    val queue: Option[String] = Some("queue-dev")
    val livyConf: LivyConf = new LivyConf()
      .set(LivyConf.LIVY_SPARK_VERSIONS, "v3")
      .set("livy.server.spark-home.v3", "/usr/share/spark-3.1")
      .set("livy.server.spark-conf-dir.v3", "/etc/spark-3.1")
      .set("livy.server.spark-home.v3.preview", "/usr/share/spark-3.2")
      .set("livy.server.spark-conf-dir.v3.preview", "/etc/spark-3.2")
      .set(LivyConf.SPARK_LIVY_SPARK_PREVIEW_ENABLED, true)
      .set(LivyConf.SPARK_LIVY_SPARK_PREVIEW_PREVENT_QUEUES, "queue-dev,others-dev")

    val sparkVersion = Session.reqSparkVersionOrPreview(reqSparkVersion, queue, livyConf)
    assert(sparkVersion.get == "v3")
  }

  test("should not use preview spark when user disabled") {
    val reqSparkVersion: Option[String] = Some("v3")
    val queue: Option[String] = Some("queue-dev")
    val livyConf: LivyConf = new LivyConf()
      .set(LivyConf.LIVY_SPARK_VERSIONS, "v3")
      .set("livy.server.spark-home.v3", "/usr/share/spark-3.1")
      .set("livy.server.spark-conf-dir.v3", "/etc/spark-3.1")
      .set("livy.server.spark-home.v3.preview", "/usr/share/spark-3.2")
      .set("livy.server.spark-conf-dir.v3.preview", "/etc/spark-3.2")
      .set(LivyConf.SPARK_LIVY_SPARK_PREVIEW_ENABLED, true)

    val sparkVersion = Session.reqSparkVersionOrPreview(reqSparkVersion, queue,
      livyConf, Some("false"))
    assert(sparkVersion.get == "v3")
  }

}
