/**
 * Bilibili.com Inc. Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.spark.metrics.event;

/**
 *
 * @author wth
 * @version $Id: ByteEvent.java, v 0.1 2020-06-12 14:47
wth Exp $$
 */
public class ByteEvent extends WrapEvent {

    private byte[] data;

    public ByteEvent() {
        data = new byte[0];
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        if (null == data) {
            data = new byte[0];
        }
        this.data = data;
    }
}
