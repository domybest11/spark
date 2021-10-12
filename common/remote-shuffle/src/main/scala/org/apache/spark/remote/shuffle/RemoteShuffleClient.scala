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

import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient, TransportClientBootstrap}
import org.apache.spark.network.server.NoOpRpcHandler
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage
import org.apache.spark.network.shuffle.protocol.remote.{GetPushMergerLocations, MergerWorkers, RemoteShuffleDriverHeartbeat, UnregisteredApplication}
import org.apache.spark.network.util.TransportConf


class RemoteShuffleClient(transportConf: TransportConf, masterHost: String, masterPort: Int) {
  private var client: TransportClient = _
  private var transportContext: TransportContext = _

  def start(): Unit = {
    require(client == null, "remote shuffle server driver client already started")
    // TODO: connect to master
    val bootstraps: util.List[TransportClientBootstrap] = new util.ArrayList[TransportClientBootstrap]()
    transportContext = new TransportContext(transportConf, new NoOpRpcHandler, true, true)
    client = transportContext.createClientFactory(bootstraps)
      .createClient(masterHost, masterPort)
  }

  def stop(): Unit = {
    if (transportContext != null) {
      transportContext.close()
      transportContext = null
    }
    if(client != null) {
      client.close()
      client = null
    }
  }

  def startApplication(appId: String, appAttemptId: Option[String]): Unit = {
    client.send(
      new UnregisteredApplication(appId, Integer.valueOf(appAttemptId.getOrElse("-1"))).toByteBuffer
    )
  }

  def stopApplication(appId: String, appAttemptId: Option[String]): Unit = {
    client.send(
      new UnregisteredApplication(appId, Integer.valueOf(appAttemptId.getOrElse("-1"))).toByteBuffer
    )
  }

  def sendHeartbeat(appId: String, appAttemptId: Option[String]): Unit = {
    client.send(
      new RemoteShuffleDriverHeartbeat(
        appId,
        Integer.valueOf(appAttemptId.getOrElse("-1")),
        100L
      ).toByteBuffer
    )
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
    var result: Seq[String] = Nil
    client.sendRpc(getPushMergerLocations.toByteBuffer, new RpcResponseCallback {
      /**
       * Successful serialized result from server.
       *
       * After `onSuccess` returns, `response` will be recycled and its content will become invalid.
       * Please copy the content of `response` if you want to use it after `onSuccess` returns.
       */
      override def onSuccess(response: ByteBuffer): Unit = {
        val msgObj = BlockTransferMessage.Decoder.fromByteBuffer(response)
        result = msgObj.asInstanceOf[MergerWorkers].getWorkerInfos
      }

      /** Exception either propagated from server or raised on client side. */
      override def onFailure(e: Throwable): Unit = {

      }
    })
    result
  }

}
