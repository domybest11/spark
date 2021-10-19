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
package org.apache.spark.remote.shuffle

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.LoggerFactory

import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient, TransportClientBootstrap}
import org.apache.spark.network.server.NoOpRpcHandler
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage
import org.apache.spark.network.shuffle.protocol.remote.{GetPushMergerLocations, MergerWorkers, RegisterApplication, RemoteShuffleDriverHeartbeat, UnregisterApplication}
import org.apache.spark.network.util.TransportConf

class RemoteShuffleClient(transportConf: TransportConf, masterHost: String, masterPort: Int) {
  private val logger = LoggerFactory.getLogger(classOf[RemoteShuffleClient])

  private var client: TransportClient = _
  private var transportContext: TransportContext = _
  private lazy val heartbeatThread = Executors.newSingleThreadScheduledExecutor(
    new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("remote-shuffle-driver-heartbeat")
      .build
  )

  def start(): Unit = {
    require(client == null, "remote shuffle server driver client already started")
    connection()
  }

  private def connection(): Unit = {
    val bootstraps = new util.ArrayList[TransportClientBootstrap]()
    transportContext = new TransportContext(transportConf, new NoOpRpcHandler, false, true)
    client = transportContext.createClientFactory(bootstraps)
      .createClient(masterHost, masterPort)
  }

  def stop(): Unit = {
    if (heartbeatThread != null) {
      heartbeatThread.shutdownNow()
    }
    if (transportContext != null) {
      transportContext.close()
      transportContext = null
    }
    if(client != null) {
      client.close()
      client = null
    }
  }

  def startApplication(appId: String,
                       appAttemptId: Option[String],
                       reportIntervalMs: Long): Unit = {
    client.sendRpc(
      new RegisterApplication(
        appId,
        Integer.valueOf(appAttemptId.getOrElse("-1"))).toByteBuffer,
      new RpcResponseCallback {
        override def onSuccess(response: ByteBuffer): Unit = {
          heartbeatThread.scheduleAtFixedRate(() => {
            sendHeartbeat(appId, Integer.valueOf(appAttemptId.getOrElse("-1")))
          }, 30, reportIntervalMs, TimeUnit.SECONDS)
          logger.info("Registered application to remote shuffle master successfully")
        }

        override def onFailure(e: Throwable): Unit = {
          logger.error("Unable to register application to remote shuffle master")
        }
      }
    )
  }

  def stopApplication(appId: String, appAttemptId: Option[String]): Unit = {
    client.send(
      new UnregisterApplication(appId, Integer.valueOf(appAttemptId.getOrElse("-1"))).toByteBuffer
    )
  }

  private def sendHeartbeat(appId: String, appAttemptId: Int): Unit = {
    try {
      client.send(
        new RemoteShuffleDriverHeartbeat(
          appId,
          appAttemptId,
          System.currentTimeMillis()
        ).toByteBuffer
      )
      logger.info("Send application heartbeat success")
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def getShufflePushMergerLocations(
     appId: String,
     appAttemptId: Option[String],
     shuffleId: Int,
     numPartitions: Int,
     tasksPerExecutor: Int,
     maxExecutors: Int): Seq[String] = {

    val getPushMergerLocations = new GetPushMergerLocations(
      appId,
      Integer.valueOf(appAttemptId.getOrElse("-1")),
      shuffleId,
      numPartitions,
      tasksPerExecutor,
      maxExecutors
    )
    if (!client.isActive) {
      connection()
    }
    val response = client.sendRpcSync(getPushMergerLocations.toByteBuffer, 1000L)
    val msgObj = BlockTransferMessage.Decoder.fromByteBuffer(response)
    val result = msgObj.asInstanceOf[MergerWorkers].getWorkerInfos
    logger.info("Shuffle {} get push merger location size: {}", shuffleId, result.size)
    result
  }

}
