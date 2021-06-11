package org.apache.spark.util.wechat;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;

public class BotResponse {
  private final int    errorCode;
  private final String errorMessage;

  @JsonCreator
  public BotResponse(@JsonProperty("errcode") int errorCode,
                     @JsonProperty("errmsg") String errorMessage) {
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("BotResponse{");
    sb.append("errorCode=").append(errorCode);
    sb.append(", errorMessage='").append(errorMessage).append('\'');
    sb.append('}');
    return sb.toString();
  }

  boolean success() {
    return errorCode == 0;
  }

  IOException toIOException() {
    return new IOException("Sending error: code=" + errorCode + ", message=" + errorMessage);
  }
}
