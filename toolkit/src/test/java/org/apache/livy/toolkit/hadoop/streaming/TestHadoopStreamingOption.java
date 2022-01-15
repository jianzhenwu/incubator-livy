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
package org.apache.livy.toolkit.hadoop.streaming;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestHadoopStreamingOption {

  @Test
  public void testHadoopStreamingOption() {
    String[] args =
        { "-input", "input1", "-input", "input2", "-output", "output",
            "-mapper", "cat", "-reducer", "cat",
            "-partitioner", "org.apache.hadoop.mapred.lib.HashPartitioner",
            "-inputformat", "org.apache.hadoop.mapred.TextInputFormat",
            "-outputformat", "org.apache.hadoop.mapred.TextOutputFormat",
            "-cmdenv", "cmd1=a", "-D", "conf1=b"};

    HadoopStreamingOption hadoopStreamingOption = new HadoopStreamingOption(args);
    assertEquals(hadoopStreamingOption.getInput()[0], "input1");
    assertEquals(hadoopStreamingOption.getInput()[1], "input2");
    assertEquals(hadoopStreamingOption.getOutput(), "output");
    assertEquals(hadoopStreamingOption.getMapper(), "cat");
    assertEquals(hadoopStreamingOption.getReducer(), "cat");
    assertEquals(hadoopStreamingOption.getPartitioner(),
        "org.apache.hadoop.mapred.lib.HashPartitioner");
    assertEquals(hadoopStreamingOption.getInputFormat(),
        "org.apache.hadoop.mapred.TextInputFormat");
    assertEquals(hadoopStreamingOption.getOutputFormat(),
        "org.apache.hadoop.mapred.TextOutputFormat");
    assertEquals(hadoopStreamingOption.getCmdEnv().get("cmd1"), "a");
    assertEquals(hadoopStreamingOption.getConf().get("conf1"), "b");
  }
}
