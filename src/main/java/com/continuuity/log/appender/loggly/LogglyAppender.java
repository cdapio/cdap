package com.continuuity.log.appender.loggly;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import java.net.MalformedURLException;
import java.net.URL;
import ch.qos.logback.core.Layout;

public final class LogglyAppender extends AppenderBase<ILoggingEvent> {
  private URL endpoint;
  private String strEndpoint;
  private LogglyPoster poster;
  private int eventQueueSize;
  private int numRetries = 3;
  private long timeoutInMillis = 3000l;
  private SloppyCircularBuffer<String> queue;
  private Layout<ILoggingEvent> layout;

  @Override
  protected void append(final ILoggingEvent event) {
    String logMsg = layout.doLayout(event);
    this.queue.enqueue(logMsg);
  }

  @Override
  public void start() {
    if(this.endpoint == null) {
      super.addError("No endpoint set for appender [" + super.name + "].");
    } else if(this.layout == null) {
      super.addError("No layout set for appender [" + super.name + "].");
    } else {
      final int queueSize = Math.max(1, this.eventQueueSize);
      this.queue = new SloppyCircularBuffer<String>(queueSize);
      this.poster = new LogglyPoster(this.endpoint, this.strEndpoint,
                                     this.queue, this.numRetries,
                                     this.timeoutInMillis);
      this.poster.start();
      super.start();
      super.addInfo("Appender [" + super.name + "] started with a " +
                      "queue size of " + queueSize);
    }
  }

  @Override
  public void stop() {
    this.poster.interrupt();
    super.stop();
  }

  public void setEndpoint(final String endpoint)
    throws MalformedURLException {
    this.endpoint = new URL(endpoint);
    this.strEndpoint = endpoint;
  }

  public void setQueueSize(final int maxSize) {
    this.eventQueueSize = maxSize;
  }

  public Layout<ILoggingEvent> getLayout() {
    return this.layout;
  }

  public void setLayout(final Layout<ILoggingEvent> layout) {
    this.layout = layout;
  }

  public int getNumRetries() {
    return numRetries;
  }

  public void setNumRetries(int numRetries) {
    this.numRetries = numRetries;
  }

  public long getTimeoutInMillis() {
    return timeoutInMillis;
  }

  public void setTimeoutInMillis(long timeoutInMillis) {
    this.timeoutInMillis = timeoutInMillis;
  }
}