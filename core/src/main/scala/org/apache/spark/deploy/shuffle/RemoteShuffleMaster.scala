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
package org.apache.spark.deploy.shuffle

import java.util.concurrent.CountDownLatch

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.util.TransportConf
import org.apache.spark.remote.shuffle.RemoteShuffleMasterHandler
import org.apache.spark.util.{ShutdownHookManager, Utils}

object RemoteShuffleMaster extends Logging {
  @volatile
  private var master: RemoteShuffleMasterHandler = _

  private val barrier = new CountDownLatch(1)

  def main(args: Array[String]): Unit = {
    main(args,
      (host: String, port: Int, conf: TransportConf) =>
        new RemoteShuffleMasterHandler(host, port, conf)
    )
  }


  /** A helper main method that allows the caller to call this with a remote shuffle master. */
  private[spark] def main(
      args: Array[String],
      remoteShuffleMaster: (String, Int, TransportConf) => RemoteShuffleMasterHandler): Unit = {
    Utils.initDaemon(log)
    val sparkConf = new SparkConf
    Utils.loadDefaultSparkProperties(sparkConf)
    // we override this value since this service is started from the command line
    // and we assume the user really wants it to be running
    sparkConf.set(config.SHUFFLE_REMOTE_SERVICE_ENABLED.key, "true")
    val host = sparkConf.get(config.SHUFFLE_REMOTE_SERVICE_MASTER_HOST)
    require(host.isDefined, "Remote shuffle master must settings")
    val port = sparkConf.get(config.SHUFFLE_REMOTE_SERVICE_MASTER_PORT)
    val transportConf =
      SparkTransportConf.fromSparkConf(sparkConf, "shuffle", numUsableCores = 0)
    master = remoteShuffleMaster(host.get, port, transportConf)
    master.start()

    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook { () =>
      logInfo("Shutting down remote shuffle master.")
      master.stop()
      barrier.countDown()
    }

    // keep running until the process is terminated
    barrier.await()
  }
}

