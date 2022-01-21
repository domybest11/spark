package org.apache.spark.util.log;

import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.apache.spark.collector.FailureJobCollector;
import org.apache.spark.metrics.event.LogErrorWrapEvent;
import org.apache.spark.util.Utils;

import java.io.IOException;
import java.util.Properties;

public class CustomRollingFileAppender extends RollingFileAppender {

  private Properties setting = System.getProperties();
  private String threadName = "driver-logError-task";
  private FailureJobCollector<LogErrorWrapEvent> failureJobCollector =
      new FailureJobCollector(setting, threadName);


  public CustomRollingFileAppender() {
    super();
    failureJobCollector.setPOISON_PILL(new LogErrorWrapEvent());
    failureJobCollector.start();
  }


  public CustomRollingFileAppender(Layout layout, String filename, boolean append)
      throws IOException {
    super(layout, filename, append);
    failureJobCollector.setPOISON_PILL(new LogErrorWrapEvent());
    failureJobCollector.start();
  }
  public CustomRollingFileAppender(Layout layout, String filename) throws IOException {
    super(layout, filename);
    failureJobCollector.setPOISON_PILL(new LogErrorWrapEvent());
    failureJobCollector.start();
  }


  protected void subAppend(LoggingEvent event) {
    if (event != null && (event.getLevel() == Level.FATAL || event.getLevel() == Level.ERROR)) {
      ThrowableInformation throwableInformation = event.getThrowableInformation();
      if (throwableInformation != null) {
        failureJobCollector.getLogErrorQueue()
            .offer(new LogErrorWrapEvent("Spark", "Error",
                setting.getProperty("spark.app.name"), setting.getProperty("spark.trace.id"),
                setting.getProperty("spark.app.id"), System.currentTimeMillis(),
                event.getMessage().toString(), Utils.exceptionString(throwableInformation.getThrowable())));
      } else if (event.getMessage() != null &&
          event.getMessage().toString().startsWith("Error in query")) {
        failureJobCollector.getLogErrorQueue()
            .offer(new LogErrorWrapEvent("Spark", "Error",
                setting.getProperty("spark.app.name"), setting.getProperty("spark.trace.id"),
                setting.getProperty("spark.app.id"), System.currentTimeMillis(),
                event.getMessage().toString(), event.getMessage().toString()));
      }
    }
    super.subAppend(event);
  }
}
