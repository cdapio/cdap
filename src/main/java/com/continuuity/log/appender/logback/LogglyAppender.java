package com.continuuity.log.appender.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;
import com.continuuity.log.appender.loggly.LogglyPoster;
import com.continuuity.log.appender.loggly.SloppyCircularBuffer;
import com.continuuity.log.common.AbstractHttpFeeder;
import com.continuuity.log.common.Feeder;

import java.net.MalformedURLException;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: nmotgi
 * Date: 9/27/12
 * Time: 8:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class LogglyAppender extends AppenderBase<ILoggingEvent> {
  /**
   * End of line, for our own internal presentation.
   *
   * <p>We use this symbol in order to separate lines in buffer, not in order
   * to show them to the user. Thus, it's platform independent symbol and
   * will work on any OS (incl. Windows).
   */
  static final String EOL = "\n";

  /**
   * Queue of messages to send to server.
   */
  private final transient BlockingQueue<String> messages =
    new LinkedBlockingQueue<String>();

  /**
   * The service to run the background process.
   */
  private final transient ScheduledExecutorService service =
    Executors.newScheduledThreadPool(1);

  /**
   * The feeder.
   */
  private transient Feeder feeder;

  private transient String feederKlass;
  private transient String feederUrl;
  private transient boolean feederSplit = false;

  private Layout<ILoggingEvent> layout;

  /**
   * The future we're running in.
   */
  private transient ScheduledFuture<?> future;

  /**
   * Set feeder, option {@code feeder} in config.
   * @param feederKlass The feeder to use
   */
  public void setFeeder(String feederKlass) {
    this.feederKlass = feederKlass;
  }

  public String getFeeder() {
    return feederKlass;
  }

  public void setUrl(String feederUrl) {
    this.feederUrl = feederUrl;
  }

  public String getUrl() {
    return feederUrl;
  }

  public void setSplit(String split) {
    if("yes".equals(split) || "YES".equals(split)) {
      feederSplit = true;
    }
  }

  public Layout<ILoggingEvent> getLayout() {
    return this.layout;
  }

  public void setLayout(final Layout<ILoggingEvent> layout) {
    this.layout = layout;
  }

  @Override
  public void start() {
    if (this.feeder == null) {
      try {
        Class<?> feederClass = Class.forName(this.feederKlass);
        AbstractHttpFeeder abstractHttpFeeder
          = (AbstractHttpFeeder)feederClass.newInstance();
        abstractHttpFeeder.setUrl(feederUrl);
        feeder = abstractHttpFeeder;
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException(e);
      } catch (InstantiationException e) {
        throw new IllegalStateException(e);
      } catch (IllegalAccessException e) {
        throw new IllegalStateException(e);
      } catch (MalformedURLException e) {
        throw new IllegalStateException(e);
      }
    }
    this.future = this.service.scheduleWithFixedDelay(
      new Runnable() {
        @Override
        public void run() {
          LogglyAppender.this.flush();
        }
      },
      1L,
      1L,
      TimeUnit.SECONDS
    );
    super.start();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop() {
    if (this.future != null) {
      this.future.cancel(true);
    }
    this.service.shutdown();
  }

  @Override
  protected void append(ILoggingEvent event) {
    final StringBuilder buf = new StringBuilder();
    buf.append(this.getLayout().doLayout(event));

    if(event.getThrowableProxy() != null) {
      final StackTraceElementProxy[] exc
        = event.getThrowableProxy().getStackTraceElementProxyArray();
      if (exc != null) {
        for (StackTraceElementProxy text : exc) {
          buf.append(text.toString()).append(EOL);
        }
      }
    }
    final boolean correctlyInserted = this.messages.offer(buf.toString());
    if (!correctlyInserted) {
      System.out.println(
        String.format(
          "LogglyAppender doesn't have space available to store the event: %s",
          buf.toString()
        )
      );
    }
  }

  /**
   * Method to be executed by a thread in the background.
   *
   * <p>Takes messages from the queue and feeds the feeder.
   */
  private void flush() {
    String text;
    try {
      text = this.messages.take();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(ex);
    }
    try {
      this.feeder.feed(text);
    } catch (java.io.IOException ex) {
      System.out.println(
        String.format(
          "%s LogglyAppender failed to report: %s",
          text,
          ex.getMessage()
        )
      );
    }
  }
}
