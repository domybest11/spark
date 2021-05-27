package org.apache.spark.status;

public class JvmPausedWrap {
  private String service = "taskPauseMonitor";
  private String appId;
  private int appAttemptId;
  private String appType = "spark";
  private String executorId;
  private long pauseTime;
  private long sendTime;

  public JvmPausedWrap(String appId, int appAttemptId, String executorId,
                       long pauseTime, long sendTime) {
    this.appId = appId;
    this.appAttemptId = appAttemptId;
    this.executorId = executorId;
    this.pauseTime = pauseTime;
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

  public long getPauseTime() {
    return pauseTime;
  }

  public void setPauseTime(long pauseTime) {
    this.pauseTime = pauseTime;
  }

  public long getSendTime() {
    return sendTime;
  }

  public void setSendTime(long sendTime) {
    this.sendTime = sendTime;
  }
}
