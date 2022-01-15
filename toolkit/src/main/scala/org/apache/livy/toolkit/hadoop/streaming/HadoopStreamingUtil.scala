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

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapred.{Partitioner => HPartitioner}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

/**
 * Utility methods used by Hadoop streaming job.
 */
object HadoopStreamingUtil {

  def getInputFormatClass[K, V](inputFormat: String): Class[_ <: InputFormat[K, V]] = {
    try {
      if (StringUtils.isNotBlank(inputFormat)) {
        Class.forName(
          inputFormat.trim,
          true,
          Thread.currentThread().getContextClassLoader)
          .asSubclass(classOf[InputFormat[K, V]])
      } else {
        classOf[KeyValueTextInputFormat].asSubclass(classOf[InputFormat[K, V]])
      }
    } catch {
      case e: ClassNotFoundException =>
        throw e
    }
  }

  def getOutputFormatClass[K, V](outputFormat: String): Class[_ <: OutputFormat[_, _]] = {
    try {
      if (StringUtils.isNotBlank(outputFormat)) {
        Class.forName(
          outputFormat.trim,
          true,
          Thread.currentThread().getContextClassLoader)
          .asSubclass(classOf[OutputFormat[K, V]])
      } else {
        classOf[TextOutputFormat[K, V]]
      }
    } catch {
      case e: ClassNotFoundException =>
        throw e
    }
  }

  def getCompressionCodecClass(codec: String): Class[_ <: CompressionCodec] = {
    try {
      if (StringUtils.isNotBlank(codec)) {
        Class.forName(
          codec.trim,
          true,
          Thread.currentThread().getContextClassLoader)
          .asSubclass(classOf[CompressionCodec])
      } else {
        null
      }
    } catch {
      case e: ClassNotFoundException =>
        throw e
    }
  }

  def getPartitioner[K, V](partitioner: String): HPartitioner[K, V] = {
    try {
      val partitionerClass = Class.forName(partitioner)
      partitionerClass.getConstructor().newInstance().asInstanceOf[HPartitioner[K, V]]
    } catch {
      case e: ClassNotFoundException =>
        throw e
    }
  }

  def rddToPairRDD(rdd: RDD[String], sep: String): RDD[(String, String)] = {
    rdd.map(r => {
      val result = r.split(sep, 2)
      if (result.size == 2) (result.apply(0), result.apply(1)) else (r, "")
    })
  }

  def pairRDDToRDD[K, V](pairRDD: RDD[(K, V)], seq: String): RDD[String] = {
    pairRDD.map(r => s"${r._1.toString}$seq${r._2.toString}")
  }

  def saveAsHadoopFile[K, V](
      conf: SparkConf,
      rdd: PairRDDFunctions[K, V],
      output: String,
      outputFormat: String): Unit = {
    val outputFormatClass = HadoopStreamingUtil.getOutputFormatClass[Text, Text](outputFormat)
    conf.getOption(s"spark.hadoop.${FileOutputFormat.COMPRESS}") match {
      case Some(compress) if "true".equals(compress) =>
        conf.getOption(s"spark.hadoop.${FileOutputFormat.COMPRESS_CODEC}") match {
          case Some(codec) =>
            val compressionCodecClass = HadoopStreamingUtil.getCompressionCodecClass(codec)
            rdd.saveAsHadoopFile(
              output,
              classOf[Text], classOf[Text],
              outputFormatClass, compressionCodecClass)
          case None =>
            rdd.saveAsHadoopFile(
              output, classOf[Text], classOf[Text], outputFormatClass)
        }
      case _ =>
        rdd.saveAsHadoopFile(output, classOf[Text], classOf[Text], outputFormatClass)
    }
  }
}
