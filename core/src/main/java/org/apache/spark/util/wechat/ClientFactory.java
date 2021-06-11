package org.apache.spark.util.wechat;

import okhttp3.OkHttpClient;
import retrofit2.Retrofit;

import java.lang.annotation.Annotation;
import java.util.concurrent.TimeUnit;

public class ClientFactory {
  public static <T> T create(Class<T> cls, String baseUrl) {
    OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();

    HttpClientAttributes attr = getHttpClientAttributes(cls);
    if (attr != null) {
      clientBuilder.readTimeout(attr.readTimeoutMillis(), TimeUnit.MILLISECONDS);
      clientBuilder.followRedirects(attr.followRedirects());
    }

    Retrofit.Builder builder = new Retrofit.Builder();
    builder.baseUrl(baseUrl);
    builder.addConverterFactory(ConverterFactory.create());
    builder.client(clientBuilder.build());
    return builder.build().create(cls);
  }

  private static HttpClientAttributes getHttpClientAttributes(Class<?> cls) {
    Annotation[] annotations = cls.getAnnotations();
    for (Annotation annotation : annotations) {
      if (annotation.annotationType().equals(HttpClientAttributes.class)) {
        return (HttpClientAttributes) annotation;
      }
    }
    return null;
  }
}
