package org.apache.spark.collector;

import org.apache.log4j.helpers.LogLog;
import org.apache.spark.SparkConf;
import org.apache.spark.metrics.event.LogErrorWrapEvent;
import org.apache.spark.metrics.event.WrapEvent;
import org.apache.spark.metrics.sink.KafkaSink;
import org.apache.spark.util.ErrorRecord;
import org.apache.spark.util.KafkaProducerUtil;
import org.apache.spark.util.Utils;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class FailureJobCollector<T extends WrapEvent>{

  private String appName;
  private T POISON_PILL;
  private Properties setting;
  private SparkConf conf;
  private KafkaSink kafkaSink;
  private String threadName = "Spark";
  private String suffix  = "-sinkKafka-logError-task";
  private int EXECUTION_INFO_QUEUE_CAPACITY = 10000;

  public LinkedBlockingQueue<T> getLogErrorQueue() {
    return logErrorQueue;
  }

  public void setLogErrorQueue(LinkedBlockingQueue<T> logErrorQueue) {
    this.logErrorQueue = logErrorQueue;
  }

  public FailureJobCollector() {

  }

  public FailureJobCollector(SparkConf conf, String threadName) {
    this.conf = conf;
    this.threadName = threadName;
  }

  public FailureJobCollector(Properties setting, String threadName) {
    SparkConf sparkConf = new SparkConf();
    Utils.loadDefaultSparkProperties(sparkConf, null);
    setConf(sparkConf);
    setting.forEach((key, value)->{
      this.conf.set(key.toString(), value.toString());
    });
    this.setting = setting;
    this.threadName = threadName;
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public T getPOISON_PILL() {
    return POISON_PILL;
  }

  public void setPOISON_PILL(T POISON_PILL) {
    this.POISON_PILL = POISON_PILL;
  }

  public Properties getSetting() {
    return setting;
  }

  public void setSetting(Properties setting) {
    this.setting = setting;
  }

  public SparkConf getConf() {
    return conf;
  }

  public void setConf(SparkConf conf) {
    this.conf = conf;
  }

  public KafkaSink getKafkaSink() {
    return kafkaSink;
  }

  public void setKafkaSink(KafkaSink kafkaSink) {
    this.kafkaSink = kafkaSink;
  }

  public String getThreadName() {
    return threadName;
  }

  public void setThreadName(String threadName) {
    this.threadName = threadName;
  }

  public int getEXECUTION_INFO_QUEUE_CAPACITY() {
    return EXECUTION_INFO_QUEUE_CAPACITY;
  }

  public void setEXECUTION_INFO_QUEUE_CAPACITY(int EXECUTION_INFO_QUEUE_CAPACITY) {
    this.EXECUTION_INFO_QUEUE_CAPACITY = EXECUTION_INFO_QUEUE_CAPACITY;
  }

  public Thread getSendLogErrorKafkaThread() {
    return sendLogErrorKafkaThread;
  }

  public void setSendLogErrorKafkaThread(Thread sendLogErrorKafkaThread) {
    this.sendLogErrorKafkaThread = sendLogErrorKafkaThread;
  }

  LinkedBlockingQueue<T> logErrorQueue =
      new LinkedBlockingQueue<T>(EXECUTION_INFO_QUEUE_CAPACITY);

  Thread sendLogErrorKafkaThread = new Thread(threadName + suffix) {
    @Override
    public void run() {
      try {
        T next = logErrorQueue.take();
        if (kafkaSink == null && next!= POISON_PILL) {
          kafkaSink = new KafkaSink(conf);
        }
        while (next != POISON_PILL) {
          kafkaSink.report(KafkaSink.METRIC_TOPIC, next);
          if (next instanceof LogErrorWrapEvent &&
                ((LogErrorWrapEvent)next).getMessage().startsWith("Error in query")) {
            LogErrorWrapEvent event = (LogErrorWrapEvent) next;
            ErrorRecord record = new ErrorRecord(
                    event.getAppId(),
                    event.getAppName(),
                    "error",
                    event.getTraceId(),
                    Utils.getCurrentUserName(),
                    event.getTimeStamp(),
                    event.getMessage());
            KafkaProducerUtil.report(record);
          }
          next = logErrorQueue.take();
        }
      } catch (InterruptedException e) {
        LogLog.warn("sending logError messages to kafka occurred errors:" + e.getMessage());
      }
    }
  };

  public void start() {
    sendLogErrorKafkaThread.setDaemon(true);
    sendLogErrorKafkaThread.start();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        stopCollector();
      }
    });
  }

  public void stopCollector() {
    logErrorQueue.offer(POISON_PILL);
    if (Thread.currentThread() != sendLogErrorKafkaThread) {
      try {
        sendLogErrorKafkaThread.join(10000);
      } catch (InterruptedException e) {
        LogLog.warn(threadName + " stop occurred errors: " + e.getMessage());
      }
    }
    LogLog.debug(threadName + " is stoping ......");
  }

}
