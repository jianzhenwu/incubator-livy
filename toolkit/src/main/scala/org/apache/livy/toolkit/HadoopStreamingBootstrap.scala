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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{Partitioner => HPartitioner}
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.SparkSession

import org.apache.livy.toolkit.hadoop.streaming._

/**
 * Hadoop streaming job bootstrap, migrate hadoop Streaming Jobs to Spark.
 */
private class HadoopStreamingBootstrap(
    conf: SparkConf,
    option: HadoopStreamingOption) {

  private val input: String = option.getInput.mkString(",")

  private val output: String = option.getOutput

  private val cmdEnv: mutable.Map[String, String] = option.getCmdEnv.asScala

  private val defaultSeparator = "\t"

  def createSession(): SparkSession = {
    option.getConf.entrySet().asScala.foreach(entry => {
      conf.set(s"spark.hadoop.${entry.getKey}", entry.getValue)
    })
    SparkSession.builder().config(conf).getOrCreate()
  }

  def execute(spark: SparkSession): Unit = {
    val inputFormatClass =
      HadoopStreamingUtil.getInputFormatClass[Text, Text](option.getInputFormat)
    val inputRDD = spark.sparkContext.hadoopFile(
      input, inputFormatClass, classOf[Text], classOf[Text])

    val mapperRDD = option.getMapper match {
      case cmd if StringUtils.isNotBlank(cmd) =>
        Some(HadoopStreamingUtil.pairRDDToRDD(inputRDD, defaultSeparator).pipe(cmd, cmdEnv))
      case _ => None
    }
    mapperRDD match {
      case Some(rddMapper) =>
        val reducerRDD = createReducerRDD(rddMapper, option.getReducer, option.getPartitioner)
        reducerRDD match {
          case Some(rddReducer) =>
            HadoopStreamingUtil.saveAsHadoopFile(
              conf,
              HadoopStreamingUtil.rddToPairRDD(rddReducer, defaultSeparator),
              output, option.getOutputFormat)
          case _ =>
            HadoopStreamingUtil.saveAsHadoopFile(
              conf,
              HadoopStreamingUtil.rddToPairRDD(rddMapper, defaultSeparator),
              output, option.getOutputFormat)
        }
      case _ =>
        HadoopStreamingUtil.saveAsHadoopFile(conf, inputRDD, output, option.getOutputFormat)
    }
  }

  private[toolkit] def createReducerRDD(
      mapperRDD: RDD[String],
      reducer: String,
      partitioner: String): Option[RDD[String]] = {
    reducer match {
      case cmd if StringUtils.isNotBlank(cmd) =>
        val shuffledRDD = createShuffledRDD(mapperRDD, partitioner)
        Some(HadoopStreamingUtil.pairRDDToRDD(shuffledRDD, defaultSeparator)
          .pipe(cmd, cmdEnv))
      case _ =>
        None
    }
  }

  private[toolkit] def createShuffledRDD[K, V, C](
      mapperRDD: RDD[String],
      partitioner: String): ShuffledRDD[String, String, String] = {
    partitioner match {
      case cmd if StringUtils.isNotBlank(cmd) =>
        new ShuffledRDD[String, String, String](
          HadoopStreamingUtil.rddToPairRDD(mapperRDD, defaultSeparator),
          new HadoopShimPartitioner(
            HadoopStreamingUtil.getPartitioner[String, String](cmd),
            defaultNumPartitions(mapperRDD)))
      case _ =>
        new ShuffledRDD[String, String, String](
          HadoopStreamingUtil.rddToPairRDD(mapperRDD, defaultSeparator),
          new HashPartitioner(defaultNumPartitions(mapperRDD)))
    }
  }

  private def defaultNumPartitions(rdd: RDD[_]): Int = {
    if (rdd.context.getConf.contains("spark.default.parallelism")) {
      rdd.context.defaultParallelism
    } else {
      rdd.getNumPartitions
    }
  }
}

private class HadoopShimPartitioner[K, V](
    val partitioner: HPartitioner[K, V],
    override val numPartitions: Int) extends Partitioner {
  require(numPartitions >= 0, s"Number of partitions ($numPartitions) cannot be negative.")

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => partitioner.getPartition(
      key.asInstanceOf[K], null.asInstanceOf[V], numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: HadoopShimPartitioner[K, V] =>
      h.numPartitions == this.numPartitions && h.partitioner == this.partitioner
    case _ =>
      false
  }

  override def hashCode: Int = 31 * numPartitions + partitioner.hashCode
}

object HadoopStreamingBootstrap {

  def main(args: Array[String]): Unit = {
    val option = new HadoopStreamingOption(args)
    val executor = new HadoopStreamingBootstrap(new SparkConf(), option)
    val spark = executor.createSession()
    executor.execute(spark)
    spark.stop()
  }
}
