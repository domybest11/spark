package org.apache.spark.metrics.event;

public class LogErrorWrapEvent extends WrapEvent {


  private String eventName;
  private String eventType;
  private String appName;
  private String traceId;
  private String appId;
  private long timeStamp;
  private String message;
  private String throwable;


  public LogErrorWrapEvent() {

  }

  public LogErrorWrapEvent(String eventName, String eventType, String appName, String traceId,
                           String appId, long timeStamp, String message, String throwable) {
    this.eventName = eventName;
    this.eventType = eventType;
    this.appName = appName;
    this.traceId = traceId;
    this.appId = appId;
    this.timeStamp = timeStamp;
    this.message = message;
    this.throwable = throwable;
  }

  public String getEventType() {
    return eventType;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  @Override
  public String getEventName() {
    return eventName;
  }

  @Override
  public void setEventName(String eventName) {
    this.eventName = eventName;
  }

  public String getThrowable() {
    return throwable;
  }

  public void setThrowable(String throwable) {
    this.throwable = throwable;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public String getTraceId() {
    return traceId;
  }

  public void setTraceId(String traceId) {
    this.traceId = traceId;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

}
