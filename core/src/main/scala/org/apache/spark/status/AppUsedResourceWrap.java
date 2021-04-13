package org.apache.spark.status;

public class AppUsedResourceWrap {
  private String service;
  private String appId;
  private int appAttemptId;
  private String appType;
  private String executorId;
  private int usedMemory;
  private float usedCpuPercent;
  private long sendTime;

  public AppUsedResourceWrap(String service, String appId, int appAttemptId, String appType,
                             String executorId, int driverUsedMemory, float driverUsedCpuPercent,
                             long sendTime) {
    this.service = service;
    this.appId = appId;
    this.appAttemptId = appAttemptId;
    this.appType = appType;
    this.executorId = executorId;
    this.usedMemory = driverUsedMemory;
    this.usedCpuPercent = driverUsedCpuPercent;
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

  public String getExecutorId() {
    return executorId;
  }

  public void setExecutorId(String executorId) {
    this.executorId = executorId;
  }

  public int getusedMemory() {
    return usedMemory;
  }

  public void setusedMemory(int usedMemory) {
    this.usedMemory = usedMemory;
  }

  public float getusedCpuPercent() {
    return usedCpuPercent;
  }

  public void setusedCpuPercent(float driverUsedCpuPercent) {
    this.usedCpuPercent = usedCpuPercent;
  }

  public long getSendTime() {
    return sendTime;
  }

  public void setSendTime(long sendTime) {
    this.sendTime = sendTime;
  }
}
