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
package org.apache.spark.util

import java.util.Properties

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.producer.{Callback, ProducerConfig, ProducerRecord, RecordMetadata}

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.status.api.v1.JacksonMessageWriter

private[spark] class KafkaProducerUtil(conf: SparkConf) extends Logging {
//  conf.get("spark.topic", "lancer_bigdata_spark_spark")
  val METRIC_TOPIC: String = "r_bdp_lancer.lancer_bigdata_spark_job"
  val producer: org.apache.kafka.clients.producer.KafkaProducer[String, Array[Byte]] = {
    val kafkaProps = new Properties
    kafkaProps.put(
      ProducerConfig.ACKS_CONFIG,
      conf.get("spark.acks", "0"))
    kafkaProps.put(
      ProducerConfig.BATCH_SIZE_CONFIG,
      conf.get("spark.batch.size", "20000"))
    kafkaProps.put(
      ProducerConfig.BUFFER_MEMORY_CONFIG,
      conf.get("spark.buffer.memory", "33554432"))
    kafkaProps.put(
      ProducerConfig.LINGER_MS_CONFIG,
      conf.get("spark.linger.ms", "500"))
    kafkaProps.put(
      ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
      conf.get("spark.max.request.size", "1048576"))
    kafkaProps.put(
      ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
      conf.get("spark.request.timeout.ms", "50000"))
    kafkaProps.put(
      ProducerConfig.RETRIES_CONFIG,
      conf.get("spark.retries", "3"))
    kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      conf.get("spark.bootstrap.servers",
        "10.69.142.34:9092,10.69.176.29:9092,10.69.176.30:9092"))
    kafkaProps.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      conf.get("spark.key.serializer", "org.apache.kafka.common.serialization.StringSerializer"))
    kafkaProps.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      conf.get("spark.value.serializer",
        "org.apache.kafka.common.serialization.ByteArraySerializer"))
    new org.apache.kafka.clients.producer.KafkaProducer[String, Array[Byte]](kafkaProps)
  }

  val callback: Callback = new Callback {
    override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
      if (e != null) {
        logWarning("send event to kafka error", e)
      }
    }
  }

  def report(key: String, value: String): Unit = {
    val record: ProducerRecord[String, Array[Byte]] = if (StringUtils.isBlank(key)) {
      new ProducerRecord(METRIC_TOPIC, null, null, value.getBytes())
    } else {
      new ProducerRecord(METRIC_TOPIC, null, key, value.getBytes())
    }
    producer.send(record)
  }

}

object KafkaProducerUtil extends Logging {
  private val kafkaProducer = new KafkaProducerUtil(SparkEnv.get.conf)
  private val mapper = {
    val objectMapper = new ObjectMapper
    objectMapper.registerModule(new DefaultScalaModule)
    objectMapper.enable(SerializationFeature.INDENT_OUTPUT)
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    objectMapper.setDateFormat(JacksonMessageWriter.makeISODateFormat)
    objectMapper
  }

  def report(record: Record): Unit = {
    try {
      val json = mapper.writeValueAsString(record)
      kafkaProducer.report(null, json)
    } catch {
      case e: Exception =>
        logWarning("convert2ProducerRecord error", e)
    }
  }
}

private[spark] sealed trait Record

private[spark] class ApplicationDataRecord(
    val appId: String,
    val appName: String,
    val attemptId: String,
    val submitHost: String,
    val queue: String,
    val diagnosis: String = "",
    val startTime: Long = 0,
    val endTime: Long = 0,
    val driverHost: String,
    val status: String,
    val user: String,
    val sparkVersion: String,
    val traceId: String) extends Record

private[spark] class ExecutionDataRecord(
    val appId: String,
    val appName: String,
    val attemptId: String,
    val traceId: String,
    val user: String,
    val executionId: Long,
    val status: String,
    val statement: String = "",
    val detail: String = "",
    val executePlan: String = "",
    val startTime: Long = 0,
    val endTime: Long = 0) extends Record

private[spark] class JobDataRecord(
   val appId: String,
   val appName: String,
   val attemptId: String,
   val traceId: String,
   val executionId: Long,
   val jobId: Int,
   val jobGroup: String,
   val description: String,
   var startTime: Long = 0,
   var endTime: Long = 0,
   val status: String,
   val numTasks: Int,
   var completedTasks: Int = 0,
   var failedTasks: Int = 0,
   var skippedTasks: Int = 0,
   var killedTasks: Int = 0,
   var numStages: Int = 0,
   var completedStages: Int = 0,
   var skippedStages: Int = 0,
   var failedStages: Int = 0) extends Record

private[spark] class StageDataRecord(
    val appName: String,
    val appId: String,
    val attemptId: Int,
    val traceId: String,
    val jobId: Set[Int],
    val stageId: Int,
    val stageName: String,
    val status: String,
    var numTasks: Int = 0,
    var numCompleteTasks: Int = 0,
    var numFailedTasks: Int = 0,
    var numKilledTasks: Int = 0,
    var numCompletedIndices: Int = 0,
    var firstTaskLaunchedTime: Long = 0,
    var submissionTime: Long = 0,
    var completionTime: Long = 0,
    var failureReason: String = "",
    var executorDeserializeTime: Long = 0,
    var executorDeserializeCpuTime: Long = 0,
    var resultSize: Long = 0,
    var jvmGcTime: Long = 0,
    var resultSerializationTime: Long = 0,
    var memoryBytesSpilled: Long = 0,
    var diskBytesSpilled: Long = 0,
    var peakExecutionMemory: Long = 0,
    var inputBytes: Long = 0,
    var inputRecords: Long = 0,
    var outputBytes: Long = 0,
    var outputRecords: Long = 0,
    var shuffleReadBytes: Long = 0,
    var executorRunTime: Long = 0,
    var executorCpuTime: Long = 0,
    var shuffleRemoteBlocksFetched: Long = 0,
    var shuffleLocalBlocksFetched: Long = 0,
    var shuffleFetchWaitTime: Long = 0,
    var shuffleRemoteBytesRead: Long = 0,
    var shuffleRemoteBytesReadToDisk: Long = 0,
    var shuffleLocalBytesRead: Long = 0,
    var shuffleReadRecords: Long = 0,
    var shuffleWriteBytes: Long = 0,
    var shuffleWriteTime: Long = 0,
    var shuffleWriteRecords: Long = 0,
    var isPushBasedShuffleEnabled: Boolean = false,
    var shuffleCorruptMergedBlockChunks: Long = 0,
    var shuffleFallbackCount: Long = 0,
    var shuffleMergedRemoteBlocksFetched: Long = 0,
    var shuffleMergedLocalBlocksFetched: Long = 0,
    var shuffleMergedRemoteChunksFetched: Long = 0,
    var shuffleMergedLocalChunksFetched: Long = 0,
    var shuffleMergedRemoteBytesRead: Long = 0,
    var shuffleMergedLocalBytesRead: Long = 0,
    var shuffleRemoteReqsDuration: Long = 0,
    var shuffleMergedRemoteReqsDuration: Long = 0,
    var shuffleBlocksPushed: Long = 0,
    var shuffleBlocksNotPushed: Long = 0,
    var shuffleBlocksCollided: Long = 0,
    var shuffleBlocksTooLate: Long = 0,
    var shuffleMergersCount: Long = 0,
    var taskAnyRate: Double = 0,
    var taskNoPrefRate: Double = 0,
    var taskProcessLocalRate: Double = 0,
    var taskRackLocalRate: Double = 0,
    var taskAvgMetrics: Option[TaskAvgMetrics] = None,
    var taskMaxMetrics: Option[TaskMaxMetrics] = None,
    val details: String = "",
    val description: String = "") extends Record

private[spark] class TaskAvgMetrics(
    val executorDeserializeTime: Double,
    val executorDeserializeCpuTime: Double,
    val executorRunTime: Double,
    val executorCpuTime: Double,
    val resultSize: Double,
    val jvmGcTime: Double,
    val resultSerializationTime: Double,
    val memoryBytesSpilled: Double,
    val diskBytesSpilled: Double,
    val peakExecutionMemory: Double,
    val inputBytesRead: Double,
    val inputRecordsRead: Double,
    val outputBytesWritten: Double,
    val outputRecordsWritten: Double,
    val shuffleBytesWritten: Double,
    val shuffleWriteTime: Double,
    val shuffleRecordsWritten: Double,
    val shuffleRemoteBlocksFetched: Double,
    val shuffleLocalBlocksFetched: Double,
    val shuffleFetchWaitTime: Double,
    val shuffleRemoteBytesRead: Double,
    val shuffleRemoteBytesReadToDisk: Double,
    val shuffleLocalBytesRead: Double,
    val shuffleRecordsRead: Double,
    val duration: Double,
    val schedulerDelay: Double,
    val avgShuffleReadBytes: Double)

private[spark] class TaskMaxMetrics(
    val executorDeserializeTime: Double,
    val executorDeserializeCpuTime: Double,
    val executorRunTime: Double,
    val executorCpuTime: Double,
    val resultSize: Double,
    val jvmGcTime: Double,
    val resultSerializationTime: Double,
    val memoryBytesSpilled: Double,
    val diskBytesSpilled: Double,
    val peakExecutionMemory: Double,
    val inputBytesRead: Double,
    val inputRecordsRead: Double,
    val outputBytesWritten: Double,
    val outputRecordsWritten: Double,
    val shuffleBytesWritten: Double,
    val shuffleWriteTime: Double,
    val shuffleRecordsWritten: Double,
    val shuffleRemoteBlocksFetched: Double,
    val shuffleLocalBlocksFetched: Double,
    val shuffleFetchWaitTime: Double,
    val shuffleRemoteBytesRead: Double,
    val shuffleRemoteBytesReadToDisk: Double,
    val shuffleLocalBytesRead: Double,
    val shuffleRecordsRead: Double,
    val duration: Double,
    val schedulerDelay: Double,
    val maxShuffleReadBytes: Double)

private[spark] class ExceptionRecord(
   val appId: String,
   val attemptId: String,
   val traceId: String,
   val executionId: String,
   val user: String,
   val errorType: String,
   val info: Exception) extends Record

object ExceptionType {
  val SHUFFLE_FAIL = "SHUFFLE_FAIL"
}


