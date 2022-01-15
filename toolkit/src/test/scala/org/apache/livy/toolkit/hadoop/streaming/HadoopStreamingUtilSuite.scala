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
package org.apache.livy.toolkit.hadoop.streaming

import org.scalatest.FunSuite

class HadoopStreamingUtilSuite extends FunSuite {

  test(s"get inputFormat Class from hadoop streaming option") {
    val inputFormat = "org.apache.hadoop.mapred.TextInputFormat"
    val inputFormatClass = HadoopStreamingUtil.getInputFormatClass(inputFormat)
    assert(inputFormatClass.getName.equals(inputFormat))
    assert(inputFormatClass.getCanonicalName.equals(inputFormat))
    assert(inputFormatClass.getSimpleName.equals("TextInputFormat"))
  }

  test(s"get outputFormat Class from hadoop streaming option") {
    val outputFormat = "org.apache.hadoop.mapred.TextOutputFormat"
    val outputFormatClass = HadoopStreamingUtil.getOutputFormatClass(outputFormat)
    assert(outputFormatClass.getName.equals(outputFormat))
    assert(outputFormatClass.getCanonicalName.equals(outputFormat))
    assert(outputFormatClass.getSimpleName.equals("TextOutputFormat"))
  }

  test(s"get compression codec Class from hadoop streaming option") {
    val compressionCodec = "org.apache.hadoop.io.compress.DefaultCodec"
    val compressionCodecClass = HadoopStreamingUtil.getCompressionCodecClass(compressionCodec)
    assert(compressionCodecClass.getName.equals(compressionCodec))
    assert(compressionCodecClass.getCanonicalName.equals(compressionCodec))
    assert(compressionCodecClass.getSimpleName.equals("DefaultCodec"))
  }

  test(s"get partitioner Class from hadoop streaming option") {
    val partitioner = "org.apache.hadoop.mapred.lib.HashPartitioner"
    val hadoopPartitioner = HadoopStreamingUtil.getPartitioner(partitioner)
    assert(hadoopPartitioner.getClass.getName.equals(partitioner))
    assert(hadoopPartitioner.getClass.getCanonicalName.equals(partitioner))
    assert(hadoopPartitioner.getClass.getSimpleName.equals("HashPartitioner"))
  }
}
