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
package org.apache.livy.toolkit.distcp.objects

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

/**
 * Custom partitioner based on the indexes array containing (partitionid,
 * number of batches within partition) Will handle missing partitions.
 */
case class CopyPartitioner(indexes: Array[(Int, Int)]) extends Partitioner {

  val indexesAsMap: Map[Int, Int] = indexes.toMap

  override val numPartitions: Int = indexes.map(_._2).sum + indexes.length

  val partitionOffsets: Map[Int, Int] = {
    indexes.scanRight((-1, numPartitions)) {
      case ((partition, maxKey), (_, previousOffset)) =>
        (partition, previousOffset - maxKey - 1)
    }.dropRight(1).toMap
  }

  override def getPartition(key: Any): Int = key match {
    case (p: Int, i: Int) =>
      if (!indexesAsMap.keySet.contains(p)) {
        throw new RuntimeException(
          s"Key partition $p of key ($p, $i) was not found in the indexes " +
            s"${indexesAsMap.keySet.mkString(", ")}."
        )
      }
      // Modulo the batch id to prevent exceptions if the batch id is out of the range
      partitionOffsets(p) + (i % (indexesAsMap(p) + 1))
    case u =>
      throw new RuntimeException(
        s"Partitioned does not support key $u. Must be (Int, Int)."
      )
  }

}

object CopyPartitioner {
  def apply(rdd: RDD[((Int, Int), CopyDefinitionWithDependencies)]): CopyPartitioner = {
    new CopyPartitioner(rdd.map(_._1).reduceByKey(_ max _).collect())
  }
}
