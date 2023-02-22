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

import org.scalatest.{BeforeAndAfter, FunSpec}

class LivyConfSpec extends FunSpec
  with BeforeAndAfter
  with org.scalatest.Matchers
  with LivyBaseUnitTestSuite{

  describe("LivyConfSpec") {
    it("should generate correct spark version mapping") {
      val livyConf = new LivyConf()
      livyConf
        .set("livy.server.spark.versions.alias", "v2, v3 ")
        .set("livy.server.spark.versions.alias.default ", " v3")
        .set("livy.server.spark.version.alias.mapping.v2->v2", "*")
        .set("livy.server.spark.version.alias.mapping.v3->v3_1", "*")
        .set("livy.server.spark.version.alias.mapping.v3->v3_2", "dev, infra,")

      val mapping = livyConf.getSparkAliasVersionMapping
      mapping should contain ("v2" -> Map("*" -> "v2"))
      mapping should contain ("v3" -> Map("infra" -> "v3_2",
        "dev" -> "v3_2", "*" -> "v3_1"))
    }
  }
}
