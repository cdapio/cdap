package com.continuuity.log.appender.log4j;

import com.continuuity.log.common.AbstractHttpFeeder;
import com.continuuity.log.common.Feeder;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.net.MalformedURLException;
import java.util.concurrent.*;

/**
 * Log appender, for loggly loggers.
 *
 * <p>Configure it in your {@code log4j.properties} like this (just an example,
 * which uses <a href="http://www.loggly.com">loggly.com</a> HTTP log
 * consuming interface):
 *
 * <pre>
 * log4j.rootLogger=WARN, LOGGLY, CONSOLE
 * log4j.appender.LOGGLY=com.continuuity.log.appender.log4j
 * log4j.appender.LOGGLY.feeder=com.continuuity.log.common.HttpFeeder
 * log4j.appender.LOGGLY.feeder.url=https://logs.loggly.com/inputs/0604e96...
 * log4j.appender.LOGGLY.layout=org.apache.log4j.PatternLayout
 * log4j.appender.LOGGLY.layout.ConversionPattern = [%5p] %t %c: %m\n
 * log4j.appender.CONSOLE=com.continuuity.log.appender.log4j
 * log4j.appender.CONSOLE.feeder=com.continuuity.log.common.ConsoleFeeder
 * log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
 * log4j.appender.CONSOLE.layout.ConversionPattern = [%5p] %t %c: %m\n
 * </pre>
 *
 * <p>You can extend it with your own feeding mechanisms. Just implement
 * the {@link Feeder} interface and add an instance of the class to the
 * appender.
 *
 * <p>The class is thread-safe.
 */
public class LogglyAppender extends AppenderSkeleton {

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

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean requiresLayout() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void activateOptions() {
    super.activateOptions();
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
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    if (this.future != null) {
      this.future.cancel(true);
    }
    this.service.shutdown();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void append(final LoggingEvent event) {
    final StringBuilder buf = new StringBuilder();
    buf.append(this.getLayout().format(event));
    final String[] exc = event.getThrowableStrRep();
    if (exc != null) {
      for (String text : exc) {
        buf.append(text).append(EOL);
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