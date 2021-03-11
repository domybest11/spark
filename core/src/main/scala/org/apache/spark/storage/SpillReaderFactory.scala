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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}

class SpillReaderFactory[K, C]() {

  def createSpillReader(spill: SpilledFile,
                        serializerManager: SerializerManager,
                        serInstance: SerializerInstance,
                        numPartitions: Int,
                        serializerBatchSize: Long,
                        conf: SparkConf,
                        hadoopConf: Configuration): SpillBasedReader[K, C] = {
    if (conf.get(config.SHUFFLE_SPILL_REMOTE_ENABLE)) {
        conf.get(config.SHUFFLE_SPILL_STOREA_TYPE) match {
          case "disk" =>
            new SpillDiskReader(spill, serializerManager, serInstance, numPartitions,
              serializerBatchSize, hadoopConf)
          case _ =>
            new SpillHdfsReader(spill, serializerManager, serInstance, numPartitions,
              serializerBatchSize, hadoopConf)
      }
    } else {
      new SpillDiskReader(spill, serializerManager, serInstance, numPartitions,
        serializerBatchSize, hadoopConf)
    }
  }
}
