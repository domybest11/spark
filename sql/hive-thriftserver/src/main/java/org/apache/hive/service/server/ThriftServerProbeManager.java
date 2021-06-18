package org.apache.hive.service.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.jdbc.HiveDriver;
import org.apache.hive.service.AlarmerUtils;
import org.apache.spark.util.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.concurrent.*;

public class ThriftServerProbeManager {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftServerProbeManager.class);

  private final Integer CHECK_SERVER_STATUS_INTERVAL_SEC = 5 * 60;
  private final static Integer TEST_QUERY_TIMEOUT_SEC = 900;
  private final static Integer CONNECTION_RETRY_TIMES = 5;
  private String host;
  private int port;
  public static volatile boolean thriftServerHealthy = true;

  // STS is already running and can submit the probe directly
  public void init(int port, String appId, String queue) {
    ScheduledExecutorService service = ThreadUtils.newDaemonSingleThreadScheduledExecutor("sts-probe-thread");
    this.port = port;
    ThriftServerProbeManager _this = this;
    LOG.info("Start to check spark thrift server");
    try {
      host = getHost();
    } catch (UnknownHostException e) {
      throw new RuntimeException("Failed to get ip");
    }
    service.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        thriftServerHealthy = _this.checkSparkThriftServersStatus();
        if (!thriftServerHealthy) {
          StringBuilder sb = new StringBuilder("thrift server 连接异常\n");
          sb.append("AppId: ").append(appId).append("\n");
          sb.append("URI: ").append(host).append(":").append(port).append("\n");
          sb.append("Queue: ").append(queue).append("\n");
          sb.append("即将下线重启");
          AlarmerUtils.alarm(sb.toString());
          LOG.error("Checked sts is not ok");
        }
      }
    }, 60, CHECK_SERVER_STATUS_INTERVAL_SEC, TimeUnit.SECONDS);
  }

  public boolean checkSparkThriftServersStatus() {
    Connection connection = null;
    try {
      connection = getConnection(host, port, CONNECTION_RETRY_TIMES);
      Statement stmt = connection.createStatement();
      if (connection != null) {
        CompletableFuture<ResultSet> future = CompletableFuture.supplyAsync(() -> {
          try {
            stmt.execute("SET spark.sql.thriftserver.scheduler.pool=high");
            return stmt.executeQuery("select 1");
          } catch (SQLException e) {
            return null;
          }
        });
        try {
          ResultSet rs = future.get(TEST_QUERY_TIMEOUT_SEC, TimeUnit.SECONDS);
          return rs != null;
        } catch (TimeoutException e) {
          return false;
        } finally {
          stmt.cancel();
          stmt.close();
        }
      } else {
        return false;
      }
    } catch (Exception e) {
      LOG.error("Get spark thrift server connect error", e);
      return false;
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          LOG.error("Close spark thrift server connect error", e);
        }
      }
    }

  }

  public Connection getConnection(String ip, Integer port, Integer retryTimes) throws Exception {
    Connection connection = null;
    Exception error = null;
    Integer cnt = 0;
    while (retryTimes-- > 0) {
      cnt++;
      try {
        connection = getConnection(ip, port);
        if (connection == null) {
          Thread.sleep(30000 * cnt);
          continue;
        }
        break;
      } catch (Exception e) {
        Thread.sleep(30000 * cnt);
        error = e;
      }
    }
    if (connection != null) {
      return connection;
    } else {
      if (error != null) {
        throw error;
      }
    }
    throw new Exception("get connection error null");
  }

  public Connection getConnection(String ip, Integer port) throws Exception {
    kerberosLogin();
    String jdbcUrl = String.format("jdbc:hive2://%s:%d/default;principal=hive/" +
        "jssz-bigdata-spark-01.host.bilibili.co@BILIBILI.CO;hive.server2.proxy.user=hive", ip, port);
    HiveDriver.class.newInstance();
    Connection connection = DriverManager.getConnection(jdbcUrl, "hive", "123456");
    return connection;
  }

  private String getHost() throws UnknownHostException {
    return Inet4Address.getLocalHost().getHostName();
  }

  public boolean getThriftServerHealthy() {
    return thriftServerHealthy;
  }

  public void kerberosLogin() {
    String userName = "hive/jssz-bigdata-spark-01.host.bilibili.co@BILIBILI.CO";
    String keyTab = "/etc/security/keytabs/spark.service.keytab";
    String krbConf = "/etc/krb5.conf";

    System.setProperty("java.security.krb5.conf", krbConf);
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "Kerberos");
    UserGroupInformation.setConfiguration(conf);
    try {
      UserGroupInformation.loginUserFromKeytab(userName, keyTab);
    } catch (IOException e) {
      LOG.error("Kerberos login failed.", e);
    }
  }
}
