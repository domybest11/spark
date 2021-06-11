package org.apache.hive.service.server;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.spi.HttpServerProvider;
import org.apache.hive.service.thriftProbeRestGetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class ThriftServerProbeServer {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftServerProbeServer.class);

  public void serverStart() throws IOException {
    HttpServerProvider provider = HttpServerProvider.provider();
    HttpServer httpserver =provider.createHttpServer(new InetSocketAddress(10010), 100);

    httpserver.createContext("/", new thriftProbeRestGetHandler());
    httpserver.setExecutor(null);
    httpserver.start();
    LOG.info("Thrift server probe already start");
  }
}
