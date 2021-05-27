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
package org.apache.spark.status;

public class AppMaxUsedResourceWrap {
  private String service;
  private String appId;
  private int appAttemptId;
  private String appType;
  private int driverAllocatedMemory;
  private int driverAllocatedVCores;
  private int maxDriverUsedMemory;
  private float maxDriverUsedCpuPercent;
  private int executorAllocatedMemory;
  private int executorAllocatedVCores;
  private int maxExecutorUsedMemory;
  private float maxExecutorUsedCpuPercent;
  private long sendTime;

  public AppMaxUsedResourceWrap(String service, String appId, String appType, int appAttemptId,
      int driverAllocatedMemory, int driverAllocatedVCores,
      int maxDriverUsedMemory, float maxDriverUsedCpuPercent,
      int executorAllocatedMemory, int executorAllocatedVCores,
      int maxExecutorUsedMemory, float maxExecutorUsedCpuPercent,
      long sendTime) {
    this.service = service;
    this.appId = appId;
    this.appAttemptId = appAttemptId;
    this.appType = appType;
    this.driverAllocatedMemory = driverAllocatedMemory;
    this.driverAllocatedVCores = driverAllocatedVCores;
    this.maxDriverUsedMemory = maxDriverUsedMemory;
    this.maxDriverUsedCpuPercent = maxDriverUsedCpuPercent;
    this.executorAllocatedMemory = executorAllocatedMemory;
    this.executorAllocatedVCores = executorAllocatedVCores;
    this.maxExecutorUsedMemory = maxExecutorUsedMemory;
    this.maxExecutorUsedCpuPercent = maxExecutorUsedCpuPercent;
    this.sendTime = sendTime;
  }

  public String getService() {
    return service;
  }

  public void setService(String service) {
    this.service = service;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public int getAppAttemptId() {
    return appAttemptId;
  }

  public void setAppAttemptId(int appAttemptId) {
    this.appAttemptId = appAttemptId;
  }

  public String getAppType() {
    return appType;
  }

  public void setAppType(String appType) {
    this.appType = appType;
  }

  public int getDriverAllocatedMemory() {
    return driverAllocatedMemory;
  }

  public void setDriverAllocatedMemory(int driverAllocatedMemory) {
    this.driverAllocatedMemory = driverAllocatedMemory;
  }

  public int getDriverAllocatedVCores() {
    return driverAllocatedVCores;
  }

  public void setDriverAllocatedVCores(int driverAllocatedVCores) {
    this.driverAllocatedVCores = driverAllocatedVCores;
  }

  public int getMaxDriverUsedMemory() {
    return maxDriverUsedMemory;
  }

  public void setMaxDriverUsedMemory(int maxDriverUsedMemory) {
    this.maxDriverUsedMemory = maxDriverUsedMemory;
  }

  public float getMaxDriverUsedCpuPercent() {
    return maxDriverUsedCpuPercent;
  }

  public void setMaxDriverUsedCpuPercent(float maxDriverUsedCpuPercent) {
    this.maxDriverUsedCpuPercent = maxDriverUsedCpuPercent;
  }

  public int getExecutorAllocatedMemory() {
    return executorAllocatedMemory;
  }

  public void setExecutorAllocatedMemory(int executorAllocatedMemory) {
    this.executorAllocatedMemory = executorAllocatedMemory;
  }

  public int getExecutorAllocatedVCores() {
    return executorAllocatedVCores;
  }

  public void setExecutorAllocatedVCores(int executorAllocatedVCores) {
    this.executorAllocatedVCores = executorAllocatedVCores;
  }

  public int getMaxExecutorUsedMemory() {
    return maxExecutorUsedMemory;
  }

  public void setMaxExecutorUsedMemory(int maxExecutorUsedMemory) {
    this.maxExecutorUsedMemory = maxExecutorUsedMemory;
  }


  public float getMaxExecutorUsedCpuPercent() {
    return maxExecutorUsedCpuPercent;
  }

  public void setMaxExecutorUsedCpuPercent(float maxExecutorUsedCpuPercent) {
    this.maxExecutorUsedCpuPercent = maxExecutorUsedCpuPercent;
  }

  public Long getSendTime() {
    return sendTime;
  }

  public void setSendTime(Long sendTime) {
    this.sendTime = sendTime;
  }
}
