package org.apache.hive.service;

import org.apache.spark.util.wechat.DefaultWechatBot;
import org.apache.spark.util.wechat.WechatBot;

public class AlarmerUtils {
  private final static String wechatBotUrl = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=274b495a-11d4-4e69-9f1c-c5230e839c49";

  public static void alarm(String content) {
    WechatBot bot = DefaultWechatBot.create(wechatBotUrl);
    try {
      bot.text(content);
    } catch (Exception e) {
    }
  }
}
