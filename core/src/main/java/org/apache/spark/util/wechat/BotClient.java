package org.apache.spark.util.wechat;

import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.POST;
import retrofit2.http.Url;

public interface BotClient {
  @POST
  Call<BotResponse> send(@Url String url, @Body BotMessage message);
}
