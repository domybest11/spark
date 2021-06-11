package org.apache.hive.service;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.hive.service.server.ThriftServerProbeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;

public class thriftProbeRestGetHandler implements HttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(thriftProbeRestGetHandler.class);

  ThriftServerProbeManager thriftServerProbeManager = new ThriftServerProbeManager();

  @Override
  public void handle(HttpExchange he) throws IOException {
    String requestMethod = he.getRequestMethod();
    if (requestMethod.equalsIgnoreCase("GET")) {
      Headers responseHeaders = he.getResponseHeaders();
      responseHeaders.set("Content-Type", "application/json");

      OutputStream responseBody = he.getResponseBody();

      // send response
      String response;

      if (thriftServerProbeManager.getThriftServerHealthy()) {
        response = "This is spark thrift server probe and server is ok.";
        he.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
      } else {
        LOG.error("spark thrift server is not ok");
        response = "This is spark thrift server probe and server is not ok.";
        he.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, 0);
      }
      responseBody.write(response.getBytes());

      responseBody.close();
    }
  }
}
