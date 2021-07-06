package org.apache.spark.util;

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
                logger.error("SQLAppStatusReporter report to lancer failed {} {}", body, e.getMessage());
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                logger.info("SQLAppStatusReporter report to lancer succeeded");
                response.close();
            }
        });
    }

    public void doCostPost(String body, String url) {
        Request.Builder builder = new Request.Builder()
                .url(url)
                .post(RequestBody.create(null, body))
                .addHeader("Content-Type", "application/json;charset=UTF-8")
                .addHeader("Accept-Encoding", "gzip");
        okHttpClient.newCall(builder.build()).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                logger.error("AppCostReporter report to lancer failed {} {}", body, e.getMessage());
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                logger.info("AppCostReporter report to lancer succeeded");
                response.code();
                response.message();
                response.close();
            }
        });
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
}
