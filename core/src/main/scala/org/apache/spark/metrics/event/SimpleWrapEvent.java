/**
 * Bilibili.com Inc. Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.spark.metrics.event;

/**
 *
 * @author wth
 * @version $Id: SimpleWrapEvent.java, v 0.1 2020-06-12 14:42
wth Exp $$
 */
public class SimpleWrapEvent<T> extends WrapEvent{

    private String eventType;
    private String eventName;
    private T data;

    public SimpleWrapEvent(String eventType, String eventName, T data) {
        this.eventType = eventType;
        this.eventName = eventName;
        this.data = data;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
