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
package org.apache.spark.storage

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.TaskContext
import org.apache.spark.internal.config
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.storage.SpillType.SpillType
import org.apache.spark.util.collection.PairsWriter

object BlockObjectWriterFactory {

  def createBlockObjectWriter(instance: SpillType, serializerInstance: SerializerInstance,
                              bufferSize: Int, writeMetrics: ShuffleWriteMetricsReporter,
                              blockManager: BlockManager, context: TaskContext,
                              hadoopConf: Configuration): PairsWriter = {
    val conf = blockManager.conf
    if (conf.get(config.SHUFFLE_SPILL_REMOTE_ENABLE)) {
      conf.get(config.SHUFFLE_SPILL_STOREA_TYPE) match {
        case "disk" =>
          val (blockId, file) =
            createDiskShuffleBlock(instance, blockManager)
          blockManager.getDiskWriter(blockId, file, serializerInstance, bufferSize, writeMetrics)
        case _ =>
          val (blockId, path) =
          createHdfsShuffleBlock(instance, blockManager, context, hadoopConf)
          blockManager.getHdfsWriter(blockId, path, serializerInstance, bufferSize,
              writeMetrics, hadoopConf, conf)
      }
    } else {
      val (blockId, file) =
        createDiskShuffleBlock(instance, blockManager)
      blockManager.getDiskWriter(blockId, file, serializerInstance, bufferSize, writeMetrics)
    }
  }

  def createHdfsShuffleBlock(instance: SpillType, blockManager: BlockManager,
          context: TaskContext, hadoopConf: Configuration): (BlockId, Path) = {
    val conf = blockManager.conf
    val diskBlockManager = blockManager.diskBlockManager
    if (instance == SpillType.OnlyMap) {
      diskBlockManager.createTempHdfsBlock(conf.get("spark.app.id"),
        conf.get("spark.app.attempt.id", "0"),
        context.stageId(), context.stageAttemptNumber,
        context.taskAttemptId, context.attemptNumber, hadoopConf)
    } else {
        diskBlockManager.createTempHdfsShuffleBlock(conf.get("spark.app.id"),
          conf.get("spark.app.attempt.id", "0"), context.stageId(),
          context.stageAttemptNumber, context.taskAttemptId, context.attemptNumber, hadoopConf)
    }
  }

  def createDiskShuffleBlock(instance: SpillType, blockManager: BlockManager):
  (BlockId, File) = {
    val diskBlockManager = blockManager.diskBlockManager
    if (instance == SpillType.OnlyMap) {
      diskBlockManager.createTempLocalBlock()
    } else {
      diskBlockManager.createTempShuffleBlock()
    }
  }

}

object SpillType extends Enumeration {
  val OnlyMap, ExtSort = Value

  type SpillType = Value
}