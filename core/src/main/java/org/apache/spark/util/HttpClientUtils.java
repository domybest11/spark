package org.apache.spark.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

public class HttpClientUtils {

    private static volatile HttpClientUtils INSTANCE;
    private OkHttpClient okHttpClient;

    private static final Logger logger = LoggerFactory.getLogger(HttpClientUtils.class);

    public static final String GZIP_ENCODE_UTF_8 = "UTF-8";

    private HttpClientUtils() {
        okHttpClient = new OkHttpClient.Builder().retryOnConnectionFailure(true).build();
    }

    public static HttpClientUtils getInstance() {
        if (INSTANCE == null) {
            synchronized (HttpClientUtils.class) {
                INSTANCE = new HttpClientUtils();
            }
        }
        return INSTANCE;
    }

    public void doPost(String body, String url) {
        Request.Builder builder = new Request.Builder()
                .url(url)
                .post(RequestBody.create(null, body))
                .addHeader("Content-Type", "application/json;charset=UTF-8")
                .addHeader("Accept-Encoding", "gzip");
        okHttpClient.newCall(builder.build()).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                logger.error("Report to lancer failed {} {}", body, e.getMessage());
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                logger.info("Report to lancer succeeded");
                response.close();
            }
        });
    }

    public Optional<String> doGet(String url, int timeout) {
        Request request = new Request.Builder()
            .url(url)
            .get()
            .build();
        CallbackFuture future = new CallbackFuture();
        Response resp = null;
        try {
          okHttpClient.newCall(request).enqueue(future);
          resp = future.get(timeout, TimeUnit.SECONDS);
          if (resp.isSuccessful()) {
            return Optional.of(resp.body().string());
          }
        } catch (Exception e) {
          logger.error("Get {} failed", url, e);
        } finally {
          if (resp != null) {
            resp.close();
          }
        }
        return Optional.empty();
    }

    public Map<String, Object> getJobHistoryMetric(String jobTag) {
      StringBuilder url = new StringBuilder();
      url.append("http://luckbear-api.bilibili.co/api/statistics/spark/").append(jobTag);
      Optional<String> resp = HttpClientUtils.getInstance().doGet(url.toString(), 10);
      if (resp.isPresent()) {
        try {
          ObjectMapper mapper = new ObjectMapper();
          Map res = mapper.readValue(resp.get(), Map.class);
          if (String.valueOf(res.get("code")).equals("200")) {
            return (Map<String, Object>) res.get("data");
          }
        } catch (Exception e) {
          logger.warn("Fetch job history metric error: " + resp.get());
        }
      }
      return null;
    }

    public int getGreyDeployLevel() {
      StringBuilder url = new StringBuilder();
      url.append("http://luckbear-api.bilibili.co/api/statistics/config/grey_level");
      try {
        Optional<String> resp = HttpClientUtils.getInstance().doGet(url.toString(), 10);
        if (resp.isPresent()) {
          return Integer.valueOf(resp.get());
        }
      } catch (Exception e) {
        logger.warn("Get grey deploy level failed");
      }
      return -1;
    }

    public static byte[] compress(String str, String encoding) {
        if (str == null || str.isEmpty()) {
            return null;
        }

        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
             GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(str.getBytes(encoding));
            return out.toByteArray();
        } catch (IOException e) {
            logger.error("gzip compress error.", e);
        }
        return null;
    }

  class CallbackFuture extends CompletableFuture<Response> implements Callback {
    public void onResponse(Call call, Response response) {
      super.complete(response);
    }
    public void onFailure(Call call, IOException e){
      super.completeExceptionally(e);
    }
  }
}
