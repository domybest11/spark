package org.apache.spark.metrics.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class HttpUtils {
  private static final Logger logger = LoggerFactory.getLogger(HttpUtils.class);

  private HttpURLConnection connection = null;
  private String httpUrl;

  public HttpUtils () {}

  public HttpUtils(String httpUrl) {
    try {
      URL url = new URL(httpUrl);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.setConnectTimeout(15000);
      connection.setReadTimeout(15000);
      connection.setDoOutput(true);
      connection.setDoInput(true);
      connection.setRequestProperty("Content-Type", "text/plain;charset=UTF-8");
    } catch (Exception e) {
      logger.warn("http util construct error " + e);
    }
  }

  public String doPost(String param) {
    StringBuffer result = new StringBuffer();
    OutputStream os = null;
    InputStream is = null;
    BufferedReader br = null;
    try {
      if (param != null && !param.equals("")) {
        os = connection.getOutputStream();
        os.write(param.getBytes("UTF-8"));
      }
      Object code = connection.getResponseCode();
      logger.info("http util send post request code is  " + code);
      if (connection.getResponseCode() == 200) {
        is = connection.getInputStream();
        if (is != null) {
          br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
          String temp = null;
          if ((temp = br.readLine()) != null) {
            result.append(temp);
          }
        }
        logger.info("http util send post request success ");
      }
    } catch (MalformedURLException e) {
      logger.error("http util send post request error " + e);
    } catch (IOException e) {
      logger.error("http util send post request error " + e);
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          logger.error("http util send post request error " + e);
        }
      }
      if (os != null) {
        try {
          os.close();
        } catch (IOException e) {
          logger.error("http util send post request error " + e);
        }
      }
      if (is != null) {
        try {
          is.close();
        } catch (IOException e) {
          logger.error("http util send post request error " + e);
        }
      }
      connection.disconnect();
    }
    return result.toString();
  }

  public static HttpUtils build(String httpUrl) {
    return new HttpUtils(httpUrl);
  }
}
