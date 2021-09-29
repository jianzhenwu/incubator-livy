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

package org.apache.livy.server

import org.scalatest.FunSpec

import org.apache.livy.{HistoryInfo, LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.utils.SparkApp

class SparkHistoryRandomSelectSpec extends FunSpec with LivyBaseUnitTestSuite {

  describe("LivyConf") {
    it("should map the real historyInfos") {
      val livyConf = new LivyConf()
      livyConf.set("livy.spark.history.name1.address", "history1")
      livyConf.set("livy.spark.history.name2.address", "history2")
      livyConf.set("livy.spark.history.name2.eventLogDir", "path2")
      livyConf.set("livy.spark.history.name1.eventLogDir", "path1")
      val infoList = livyConf.historyInfoList
      assert(infoList.length == 2, "The history info list size should be 2")
      val history1 = HistoryInfo("name1", "history1", "path1")
      val history2 = HistoryInfo("name2", "history2", "path2")
      assert(infoList(0) == history1)
      assert(infoList(1) == history2)
    }

    it("should throw assert error if missing some history info configuration") {
      val livyConf = new LivyConf()
      livyConf.set("livy.spark.history.name1.address", "history1")
      livyConf.set("livy.spark.history.name2.address", "history2")
      livyConf.set("livy.spark.history.name1.eventLogDir", "path1")
      assertThrows[AssertionError] {
        livyConf.historyInfoList
      }
    }

    it("Empty configuration should return 0 historyInfo") {
      val livyConf = new LivyConf()
      assert(livyConf.historyInfoList.isEmpty)
    }
  }

  describe("SparkApp") {
    val livyConf = new LivyConf()
    livyConf.set("livy.spark.master", "yarn")
    livyConf.set("livy.spark.history.name1.address", "history1")
    livyConf.set("livy.spark.history.name2.address", "history2")
    livyConf.set("livy.spark.history.name2.eventLogDir", "path2")
    livyConf.set("livy.spark.history.name1.eventLogDir", "path1")

    it("should select the random history from historyList and add to sparkConf") {
      val infoList = livyConf.historyInfoList
      val sparkConf = SparkApp.prepareSparkConf(
        "livy-test-tag",
        livyConf,
        Map())
      assert(sparkConf.getOrElse("spark.yarn.tags", "").split(",").exists(_.contains("SHS")))
      assert(sparkConf.contains("spark.yarn.historyServer.address"))
      assert(sparkConf.contains("spark.eventLog.dir"))

      val addressFromTag = sparkConf
        .getOrElse("spark.yarn.tags", "")
        .split(",")
        .filter(_.contains("SHS"))
        .apply(0)
      val addressFromSparkConf = sparkConf.getOrElse("spark.yarn.historyServer.address", "")

      assert(addressFromTag == "SHS:" + addressFromSparkConf)

      val eventLogDir = sparkConf.getOrElse("spark.eventLog.dir", "")
      assert(infoList.count(
        historyInfo =>
          historyInfo.address == addressFromSparkConf && historyInfo.eventLogDir == eventLogDir
      ) == 1)
    }
  }
}
