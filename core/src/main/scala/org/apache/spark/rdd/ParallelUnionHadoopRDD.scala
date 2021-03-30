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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{InputFormat, InputSplit, JobConf}
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.SerializableConfiguration




case class PartitionInfo(path: String, ifc: Class[InputFormat[Writable, Writable]])

private[spark] class ParallelUnionHadoopRDD[T: ClassTag](
  @transient sc: SparkContext,
  rdds: Seq[RDD[T]],
  broadcastedConf: Broadcast[SerializableConfiguration],
  initLocalJobConfFuncOpt: Option[(String, JobConf) => Unit],
  partitionInfos: Seq[PartitionInfo]) extends UnionRDD[T](sc, rdds) {

  val threshold = sc.conf.getInt("spark.rdd.parallelPartitionsThreshold", 31)

  override def getPartitions: Array[Partition] = {
    // select the latest partition input format class
    val className = partitionInfos.last.ifc.getName
    if (partitionInfos.size > threshold) {
      // Create local references so that the outer object isn't serialized.
      val rddIdMap = rdds.zipWithIndex.map(x => x._2 -> x._1.firstParent.firstParent.id).toMap
      val broadcastedJobConf = broadcastedConf
      val initJobConfFuncOpt = initLocalJobConfFuncOpt
      val partitionsWithIndex = partitionInfos.zipWithIndex.toArray

      val rddIndexWithPartitions =
        sc.parallelize(partitionsWithIndex, partitionInfos.size).map { case (part, index) =>
          val jobConfCacheKey = "rdd_%d_job_conf".format(rddIdMap(index))
          val conf = broadcastedJobConf.value.value
          val jobConf = new JobConf(conf)
          initJobConfFuncOpt.map(f => f(part.path, jobConf))
          HadoopRDD.putCachedMetadata(jobConfCacheKey, jobConf)
          SparkHadoopUtil.get.addCredentials(jobConf)

          val inputFormat =
            ReflectionUtils.newInstance(part.ifc.asInstanceOf[Class[_]], jobConf)
              .asInstanceOf[InputFormat[Writable, Writable]]
          inputFormat match {
            case c: Configurable => c.setConf(jobConf)
            case _ =>
          }
          val inputSplits = inputFormat.getSplits(jobConf, 1)
          val array = new Array[HadoopPartition](inputSplits.size)
          for (i <- 0 until inputSplits.size) {
            array(i) = new HadoopPartition(rddIdMap(index), i,
              new SerializableWritable[InputSplit](inputSplits(i)))
          }
          new SerializableHadoopPartition(index, array)
        }.collect()

      val array = new Array[Partition](rddIndexWithPartitions.map(_.splits.size).sum)
      var pos = 0

      rddIndexWithPartitions.foreach { s =>
        val rddIndex = s.rddIndex
        val splits: Array[HadoopPartition] = s.splits
        val rdd = rdds(rddIndex)
        // UnionRDD's -> firstParent -> firstParent is HadoopRDD
        val hadoopRDD = rdd.firstParent.firstParent
        hadoopRDD.setPartitions(splits.asInstanceOf[Array[Partition]])
        splits.foreach { part =>
          array(pos) = new UnionPartition(pos, rdd, rddIndex, part.index)
          pos += 1
        }
      }
      array
    } else {
      super.getPartitions
    }
  }

}
