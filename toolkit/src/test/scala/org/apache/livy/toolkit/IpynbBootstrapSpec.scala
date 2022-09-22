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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.scalatest.FunSpec

class IpynbBoostrapSpec extends FunSpec {
  val sparkConf = new SparkConf()
  val conf = new Configuration()

  describe("IpynbBootstrap parse code") {
    it("should return None for codeType if the first line is not for codeType") {
      withBootstrap { ipynbBootstrap =>
        val noTypeSources = Array(
          "print(1)\n",
          "print(2)"
        )
        val (codeType, codeStrings) = ipynbBootstrap.parseCode(noTypeSources)
        assert(codeType === None)
        assert(codeStrings === "print(1)\nprint(2)")
      }
    }

    it("should return related codeType if specified") {
      withBootstrap { ipynbBootstrap =>
        val typeSources = Array(
          "%%python\n",
          "print(1)"
        )
        val (codeType, codeStrings) = ipynbBootstrap.parseCode(typeSources)
        assert(codeType === Option("python"))
        assert(codeStrings === "print(1)")
      }
    }

    it("should return empty code if there is no sources") {
      withBootstrap { ipynbBootstrap =>
        val emptySources = Array[String]()
        val (codeType, codeStrings) = ipynbBootstrap.parseCode(emptySources)
        assert(codeType === None)
        assert(codeStrings === "")
      }
    }

    it("should skip invalid spark magic") {
      withBootstrap { ipynbBootstrap =>
        val sourcesWithInvalidMagic = Array(
          "%%invalid magic\n",
          "print(1)"
        )
        val (codeType, codeStrings) = ipynbBootstrap.parseCode(sourcesWithInvalidMagic)
        assert(codeType === None)
        assert(codeStrings === "print(1)")
      }
    }

    it("should skip unexpected spark magic") {
      withBootstrap { ipynbBootstrap =>
        val sourcesWithInvalidMagic = Array(
          "print(1)\n",
          "%matplot plt\n",
          "%unexpected magic"
        )
        val (codeType, codeStrings) = ipynbBootstrap.parseCode(sourcesWithInvalidMagic)
        assert(codeType === None)
        assert(codeStrings === "print(1)\n%matplot plt\n")
      }
    }

    it("should preserve quotes inside sources") {
      withBootstrap { ipynbBootstrap =>
        val sources = Array(
          "%%sql\n",
          "select \"a\", \"b\""
        )
        val (codeType, codeStrings) = ipynbBootstrap.parseCode(sources)
        assert(codeType === Option("sql"))
        assert(codeStrings === """select "a", "b"""")
      }
    }

    it("should catch real language magic") {
      withBootstrap { ipynbBootstrap =>
        val sources = Array(
          "%%SET a=\"dev\"\n",
          "%%sql\n",
          "select \"a\", \"b\""
        )
        val (codeType, codeStrings) = ipynbBootstrap.parseCode(sources)
        assert(codeType === Option("sql"))
        assert(codeStrings === """select "a", "b"""")
      }
    }
  }

  describe("IpynbBootstrap convert %%sql magic to pyspark") {
    it("should handle empty code") {
      withBootstrap { ipynbBootstrap =>
        val pysparkCodes = ipynbBootstrap.convertSqlMagicToPyspark("")
        assert(pysparkCodes === "")
      }
    }

    it("should convert sql to pyspark") {
      withBootstrap { ipynbBootstrap =>
        val pysparkCodes = ipynbBootstrap.convertSqlMagicToPyspark("select 1")
        assert(pysparkCodes === "spark.sql(\"\"\" select 1 \"\"\").show(20)")
      }
    }

    it("should preserve quotes inside sql") {
      withBootstrap { ipynbBootstrap =>
        val pysparkCodes = ipynbBootstrap.convertSqlMagicToPyspark(
          """
            |select "a",
            |"b"
            |""".stripMargin)
        assert(pysparkCodes === "spark.sql(\"\"\" \nselect \"a\",\n\"b\"\n \"\"\").show(20)")
      }
    }
  }

  private def withBootstrap(fn: IpynbBootstrap => Unit) = {
    // create a IpynbBootstrap
    val bootstrap = new IpynbBootstrap(sparkConf, conf)
    fn(bootstrap)
  }
}
