package org.apache.spark.util.wechat;

import java.io.IOException;

public interface WechatBot {
  void text(String text, Mentioned... mentions) throws IOException;

  /** markdown 暂不支持 Mentioned */
  void markdown(String markdown) throws IOException;

  class Mentioned {
    private final String ref;
    private final Type   type;

    enum Type {
      /** */
      USER, PHONE
    }

    private Mentioned(String ref, Type type) {
      this.ref = ref;
      this.type = type;
    }

    public static Mentioned phone(String phone) {
      return new Mentioned(phone, Type.PHONE);
    }

    public static Mentioned user(String user) {
      return new Mentioned(user, Type.USER);
    }

    String getRef() {
      return ref;
    }

    Type getType() {
      return type;
    }
  }
}
