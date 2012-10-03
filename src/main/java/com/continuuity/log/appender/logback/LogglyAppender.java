package com.continuuity.log.appender.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;
import com.continuuity.log.common.AbstractHttpFeeder;
import com.continuuity.log.common.Feeder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.*;

/**
 * Log appender, for loggly.
 *
 * <p>Configure it in your {@code log4j.properties} like this (just an example,
 * which uses <a href="http://www.loggly.com">loggly.com</a> HTTP log
 * consuming interface):
 *
 * <pre>
 *    <configuration debug="true">
 *      <appender name="loggly" class="com.continuuity.log.appender.logback.LogglyAppender">
 *        <layout>
 *           <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
 *        </layout>
 *        <feeder>com.continuuity.log.common.HttpFeeder</feeder>
 *        <split>no</split>
 *        <url>https://logs.loggly.com/inputs/17b645ec-091d-47d3-a2b8-8c2f6c4b45bf</url>
 *     </appender>
 *     <root level="ALL">
 *       <appender-ref ref="loggly" />
 *     </root>
 *   </configuration>
 * </pre>
 *
 * <p>You can extend it with your own feeding mechanisms. Just implement
 * the {@link Feeder} interface and add an instance of the class to the
 * appender.
 *
 * <p>The class is thread-safe.
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
  private static final BlockingQueue<String> messages =
    new LinkedBlockingQueue<String>(10000);

  /**
   * The service to run the background process.
   */
  private static final ScheduledExecutorService service =
    Executors.newScheduledThreadPool(1);

  /**
   * The feeder.
   */
  private transient Feeder feeder;

  /**
   * Class for the feeder.
   */
  private transient String klass;

  /**
   * Url to which the logs are sent to.
   */
  private transient String url;

  /**
   * In a multiline log, should the logs be split.
   */
  private transient boolean split = false;

  /**
   * Layout handler.
   */
  private Layout<ILoggingEvent> layout;

  /**
   * The future we're running in.
   */
  private transient ScheduledFuture<?> future;

  /**
   * Specifies whether the logger has been stopped or no.
   */
  private boolean stopped = false;

  /**
   * Set feeder, option {@code feeder} in config.
   * @param feederKlass The feeder to use
   */
  public void setFeeder(String feederKlass) {
    this.klass = feederKlass;
  }

  /**
   * @return class name for feeder
   */
  public String getFeeder() {
    return klass;
  }

  /**
   * @param url sets the url of loggly to which the logs are sent.
   */
  public void setUrl(String url) {
    this.url = url;
  }

  /**
   * @return the loggly url.
   */
  public String getUrl() {
    return url;
  }

  /**
   * Sets whether a multiline log should be split into seperate lines.
   * @param split YES to split; anything else for not.
   */
  public void setSplit(String split) {
    if("yes".equals(split) || "YES".equals(split)) {
      this.split = true;
    }
  }

  /**
   * @return Layout associated with logger.
   */
  public Layout<ILoggingEvent> getLayout() {
    return this.layout;
  }

  /**
   * Sets the layout.
   *
   * @param layout associated with logger.
   */
  public void setLayout(final Layout<ILoggingEvent> layout) {
    this.layout = layout;
  }

  /**
   * Initializes the logger.
   */
  @Override
  public void start() {
    if (this.feeder == null) {
      try {
        // Read in the class and construct the feeder.
        Class<?> feederClass = Class.forName(this.klass);
        AbstractHttpFeeder abstractHttpFeeder
          = (AbstractHttpFeeder)feederClass.newInstance();
        abstractHttpFeeder.setUrl(url);
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

    // Start the scheduler that will be flushing queue every second.
    this.future = this.service.scheduleWithFixedDelay(
      new Runnable() {
        @Override
        public void run() {
          LogglyAppender.this.flush();
        }
      },
      0L,
      500L,
      TimeUnit.MILLISECONDS
    );

    super.start();

    stopped = false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop() {
    stopped = true;
    if (this.future != null) {
      this.future.cancel(true);
    }
    while(! messages.isEmpty()) {
      System.out.println("Loggly appender waiting to flush.");
      flush();
    }
    this.service.shutdown();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void append(ILoggingEvent event) {
    // if logger has been requested to stop, we don't
    // take any more logging append requests.
    if(stopped) {
      return;
    }

    // Build the message to be logged and add it to queue.
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
   * <p>Takes messages from the queue and feeds the feeder.</p>
   */
  private void flush() {
    String text = "";
    try {
      int count = 1000;
      while(! messages.isEmpty()) {
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
