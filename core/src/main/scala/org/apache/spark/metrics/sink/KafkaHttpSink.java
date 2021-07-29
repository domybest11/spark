package org.apache.spark.metrics.sink;

import org.apache.spark.status.AppMaxUsedResourceWrap;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaHttpSink {
  private static final Logger logger = LoggerFactory.getLogger(KafkaHttpSink.class);

  private static final AppMaxUsedResourceWrap POISON_PILL = new AppMaxUsedResourceWrap("", "", "",
      0, 0, 0, 0, 0,0.0f,
      0, 0, 0,0, 0.0f, 0, "");

  private ObjectMapper objectMapper = new ObjectMapper();
  private final BlockingQueue<Object> metricQueue = new LinkedBlockingQueue<Object>();
  private Thread metricHandlingThread;
  private String url;
  private String logId;

  public KafkaHttpSink() {
  }

  public KafkaHttpSink(String url, String logId) {
    this.url = url;
    this.logId = logId;
  }

  public void produce(Object object) {
    metricQueue.add(object);
  }

  public void start() {
    metricHandlingThread = new Thread(createThread());
    metricHandlingThread.setDaemon(true);
    metricHandlingThread.setName("Async metric sink");
    metricHandlingThread.setDaemon(true);
    metricHandlingThread.start();
  }

  public void handlerMetric(Object metric) {
    try {
      metricQueue.put(metric);
    } catch (InterruptedException e) {
      logger.error("put metric to metric queue error, " + e);
    }
  }

  Runnable createThread() {
    return new Runnable() {
      @Override
      public void run() {
        Object object= null;
        try {
          object = metricQueue.take();
        } catch (InterruptedException e) {
          logger.error("take from metric queue error, " + e);
        }
        while (object != POISON_PILL) {
          try {
            consume(object);
            object = metricQueue.take();
          } catch(InterruptedException e) {
            logger.error("take from metric queue error, " + e);
          }
        }
      }
    };
  }

  public void consume(Object object) {
    long time = System.currentTimeMillis();
    String message = logId + time;
    try {
      message += objectMapper.writeValueAsString(object);
    } catch (IOException e) {
      logger.error("transfer metric to json error, " + e);
    }
    if (null != message) {
      HttpUtils.build(url).doPost(message);
    }
  }

  public boolean stop() {
    metricQueue.add(POISON_PILL);
    if (Thread.currentThread() != metricHandlingThread) {
      try {
        metricHandlingThread.join(3000);
      } catch (InterruptedException e) {
        logger.error("metricHandlingThread already interrupted, " + e);
      }
    }
    if (metricQueue.size() == 0) {
      return true;
    }
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      logger.warn("stop kafka http sink error" + e);
    }

    if (metricQueue.size() == 0) {
      return true;
    } else {
      logger.warn("kafka http sink metric queue has " + metricQueue.size() + " message");
      return false;
    }
  }
}
