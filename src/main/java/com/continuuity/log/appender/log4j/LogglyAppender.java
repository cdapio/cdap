package com.continuuity.log.appender.log4j;

import com.continuuity.log.common.AbstractHttpFeeder;
import com.continuuity.log.common.Feeder;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.io.IOException;
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
public class  LogglyAppender extends AppenderSkeleton {

  /**
   * End of line, for our own internal presentation.
   *
   * <p>We use this symbol in order to separate lines in buffer, not in order
   * to show them to the user. Thus, it's platform independent symbol and
   * will work on any OS (incl. Windows).
   */
  static final String EOL = "\n";

  /**
   * Class implementing a scheduled flush to loggly.
   */
  private class ScheduledFlush extends AbstractScheduledService {
    /**
     * Run one iteration of the scheduled task. If any invocation of this
     * method throws an exception,
     * the service will transition to the {@link com.google.common.util
     * .concurrent.Service.State#FAILED} state and this method will no
     * longer be called.
     */
    @Override
    protected void runOneIteration() throws Exception {
      try {
        flush();
      } catch (Exception e) {
        System.out.println("Problem executing flush. Reason : " +
          e.getMessage());
      }
    }

    /**
     * Returns the {@link com.google.common.util.concurrent
     * .AbstractScheduledService.Scheduler} object used to configure this
     * service.  This method will only be
     * called once.
     */
    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedDelaySchedule(0L, 500L, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Queue of messages to send to server.
   */
  private static final BlockingQueue<String> messages =
    new LinkedBlockingQueue<String>(10000);

  /**
   * Scheduled flush to loggly.
   */
  private ScheduledFlush scheduleFlush = new ScheduledFlush();

  /**
   * The feeder.
   */
  private transient Feeder feeder;

  private transient String feederKlass;
  private transient String feederUrl;
  private transient boolean feederSplit = false;

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
    scheduleFlush.start();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    // Throws no checked exception.
    scheduleFlush.stopAndWait();
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
   * <p>Takes messages from the queue and feeds the feeder.</p>
   */
  private void flush() {
    String text = "";
    try {
      int count = 1000;
      while(!this.messages.isEmpty()) {
        text = messages.poll();
        if(text == null || count < 1) {
          break;
        }
        this.feeder.feed(text);
        --count;
      }
    } catch (IOException ex) {
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