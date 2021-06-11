package org.apache.spark.util.wechat;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BotMessage {
  @JsonProperty("msgtype")
  private final String type;
  @JsonIgnore
  private final Object body;

  private BotMessage(String type, Object body) {
    this.type = type;
    this.body = body;
  }

  static BotMessage text(String content) {
    return new BotMessage("text", new Text(content, null, null));
  }

  static BotMessage text(String content, List<String> mentionedList,
                         List<String> mentionedMobileList) {
    return new BotMessage("text", new Text(content, mentionedList, mentionedMobileList));
  }

  static BotMessage markdown(String content) {
    return new BotMessage("markdown", new Markdown(content));
  }

  @JsonAnyGetter
  public Map<String, Object> any() {
    return Collections.singletonMap(type, body);
  }

  private static class Text {
    @JsonProperty
    private final String       content;

    @JsonProperty("mentioned_list")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<String> mentionedList;

    @JsonProperty("mentioned_mobile_list")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<String> mentionedMobileList;

    private Text(String content, List<String> mentionedList, List<String> mentionedMobileList) {
      this.content = content;
      this.mentionedList = mentionedList;
      this.mentionedMobileList = mentionedMobileList;
    }
  }

  private static class Markdown {
    @JsonProperty
    private final String content;

    private Markdown(String content) {
      this.content = content;
    }
  }
}
