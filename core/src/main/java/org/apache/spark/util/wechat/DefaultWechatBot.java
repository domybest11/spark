package org.apache.spark.util.wechat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DefaultWechatBot implements WechatBot {
  private final BotClient botClient;
  private final String    url;

  private DefaultWechatBot(BotClient botClient, String webHookUrl) {
    this.botClient = botClient;
    this.url = webHookUrl;
  }

  public static WechatBot create(String url) {
    BotClient client = ClientFactory.create(BotClient.class, "http://fake-url/");
    return new DefaultWechatBot(client, url);
  }

  @Override
  public void text(String content, Mentioned... mentions) throws IOException {
    if (mentions == null || mentions.length == 0) {
      send(BotMessage.text(content));
    } else {
      List<String> users = Arrays.stream(mentions)
          .filter(m -> Mentioned.Type.USER.equals(m.getType())).map(Mentioned::getRef)
          .collect(Collectors.toList());
      List<String> phones = Arrays.stream(mentions)
          .filter(m -> Mentioned.Type.PHONE.equals(m.getType())).map(Mentioned::getRef)
          .collect(Collectors.toList());
      send(BotMessage.text(content, users, phones));
    }
  }

  @Override
  public void markdown(String content) throws IOException {
    send(BotMessage.markdown(content));
  }

  private void send(BotMessage message) throws IOException {
    BotResponse resp = botClient.send(url, message).execute().body();
    if (resp != null && !resp.success()) {
      throw resp.toIOException();
    }
  }
}
