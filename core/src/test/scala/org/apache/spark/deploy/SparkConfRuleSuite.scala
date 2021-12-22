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

package org.apache.spark.deploy

import java.util

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config
import org.apache.spark.util.HttpClientUtils

class SparkConfRuleSuite extends SparkFunSuite with BeforeAndAfterEach {

  private var conf: SparkConf = null
  private var sparkConfHelper: SparkConfHelper = null
  private val metric = new util.HashMap[String, Object]()

  override def beforeAll(): Unit = {
    conf = new SparkConf()
    val httpClient = mock(classOf[HttpClientUtils])
    when(httpClient.getJobHistoryMetric(any())).thenReturn(metric)
    sparkConfHelper = new SparkConfHelper(conf, httpClient)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    conf.set("spark.deploy.autoConf", "true")
    conf.set("spark.deploy.appRetryTimes", "0")
    conf.set("spark.deploy.jobTag", "test")
    conf.set(config.EXECUTOR_MEMORY.key, "16g")
    conf.set(config.EXECUTOR_MEMORY_OVERHEAD.key, "2g")
    metric.put("executorMemory", Integer.valueOf(16384))
    metric.put("peakExecutorMemory", Integer.valueOf(1024))
  }

  test("Test executor memory rule in the case of reduce memory") {
    for (_ <- 0 until 10) {
      sparkConfHelper.applySparkConf
      metric.put("executorMemory",
        Integer.valueOf(conf.getSizeAsMb(config.EXECUTOR_MEMORY.key).toInt))
    }
    assert(metric.get("executorMemory").toString == "4096")
    assert(conf.getSizeAsGb(config.EXECUTOR_MEMORY.key) == 4)
  }

  test("Test executor memory rule in the case of increase memory") {
    metric.put("executorMemory", Integer.valueOf(18432))
    metric.put("peakExecutorMemory", Integer.valueOf(18000))
    for (_ <- 0 until 10) {
      sparkConfHelper.applySparkConf
      metric.put("executorMemory",
        Integer.valueOf(conf.getSizeAsMb(config.EXECUTOR_MEMORY.key).toInt))
    }
    assert(metric.get("executorMemory").toString == "20480")
    assert(conf.getSizeAsGb(config.EXECUTOR_MEMORY.key) == 20)
  }
}
