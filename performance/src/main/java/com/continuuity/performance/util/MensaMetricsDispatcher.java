package com.continuuity.performance.util;

import com.continuuity.common.metrics.MetricsClientProtocolHandler;
import com.continuuity.common.metrics.codec.MetricCodecFactory;
import org.apache.commons.lang.time.StopWatch;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Dispatcher for handling sending metrics to the overlord server.
 * It runs in a seperate thread dequeuing the commands written by
 * the client metric collector. If it's unable to connect to the
 * server, it implements a exponential backoff to make sure we don't
 * tax the server trying to connect.
 */
public final class MensaMetricsDispatcher implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(MensaMetricsDispatcher.class);
  /**
   * Connection timeout.
   */
  private static final long CONNECT_TIMEOUT = 100 * 1000L;

  /**
   * Specifies the maximum back-off time (in seconds).
   */
  private static final int BACKOFF_MAX_TIME = 30;

  /**
   * Specifies the minimum back-off time (in seconds).
   */
  private static final int BACKOFF_MIN_TIME = 1;

  /**
   * Specifies the exponent to be used for backing off.
   */
  private static final int BACKOFF_EXPONENT = 2;

  /**
   * Hostname to connect to.
   */
  private final String hostname;

  /**
   * Port to connect to.
   */
  private final int port;

  /**
   *
   */
  private volatile boolean keepRunning = true;

  /**
   *
   */
  private final StopWatch watcher = new StopWatch();

  /**
   * Session associated with the current connection.
   */
  private IoSession session = null;

  /**
   * Queue that holds metrics.
   */
  private final LinkedBlockingDeque<String> queue;

  /**
   * TCP connector.
   */
  private NioSocketConnector connector;

  /**
   * Constructs and initializes {@link MensaMetricsDispatcher}.
   */
  public MensaMetricsDispatcher(String hostname, int port, LinkedBlockingDeque<String> queue) {

    this.hostname = hostname;

    this.port = port;

    // Creates the queue that holds the metrics to be dispatched
    // to the overlord.
    this.queue = queue;

    // Prepare the connection.
    connector = new NioSocketConnector();

    // Sets aggressive connection timeout.
    connector.setConnectTimeoutMillis(CONNECT_TIMEOUT);

    // add an IoFilter .  This class is responsible for converting the incoming and
    // outgoing raw data to MetricRequest and MetricResponse objects
    ProtocolCodecFilter protocolFilter = new ProtocolCodecFilter(new MetricCodecFactory(true));

    // Set the protocol filter to metric codec factory.
    connector.getFilterChain().addLast("protocol", protocolFilter);

    // Set Keep Alive.
    connector.getSessionConfig().setKeepAlive(true);

    // Set to send packets of any size. As our requests are small,
    // we don't want them to be batched.
    connector.getSessionConfig().setTcpNoDelay(true);

    // Attach a handler.
    connector.setHandler(new MetricsClientProtocolHandler());

    //LOG.debug(StackTraceUtil.toStringStackTrace(e));

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        // Dispose the connector.
        if (connector != null) {
          connector.dispose();
          connector = null;
        }

        // Close the session.
        if (session != null) {
          session.close(true).awaitUninterruptibly(CONNECT_TIMEOUT);
          session = null;
        }
      }
    });
  }

  /**
   * Stops the running thread.
   */
  public void stop() {
    keepRunning = false;
    watcher.stop(); // stop the watcher.
  }

  @Override
  public void run() {
    int interval = BACKOFF_MIN_TIME;

    watcher.start();  // Start the timer.

    // While we are not asked to stop and queue
    // is not empty, we keep on going.
    while (keepRunning) {
      // Try to get a session while session is not created
      // or if created and is not connected.
      while (session == null || !session.isConnected()){
        // Make an attempt to connect, it does so by getting the
        // latest endpoint from service disocvery and tries to
        // connect to it.
        connect();

        if (session == null || (session != null && !session.isConnected())) {
          // Sleep based on how much ever is interval set to.
          try {
            LOG.warn("Backing off after unable to connect to metrics " + "collector host {}:{} for {}s.",
                     hostname, port, interval);
            Thread.sleep(interval * 1000L);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break; // Go back to check if we have asked to stop.
          }

          // Exponentially increase the amount of time to sleep,
          // untill we reach 30 seconds sleep between reconnects.
          interval = Math.min(BACKOFF_MAX_TIME, interval * BACKOFF_EXPONENT);
        } else {
          // we are conected and now need to send data.
          break;
        }
      }
      // We pop the metric to be send from the queue and
      // then attempt to send it over. If we fail and there
      // is space available we write the metric back into
      // the queue.
      final String cmd;
      String element;
      try {
        // blocking call will wait till there is an element in the queue.
        element = queue.take();
      } catch (InterruptedException e) {
        // this exception is expected when metrics reporter thread stops its dispatcher thread
        Thread.currentThread().interrupt();
        continue;
      }

      // Make sure we have not received a null object. This is
      // just a precaution.
      if (element == null) {
        continue;
      }
      cmd = element;

      // Write the command to the session and attach a future for reporting
      // any issues seen.
      LOG.debug("Trying to send metric command {} to overlord.", cmd);
      WriteFuture future = session.write(cmd);
      if (future != null) {
        future.addListener(new IoFutureListener<WriteFuture>() {
          @Override
          public void operationComplete(WriteFuture future) {
            if (!future.isWritten()) {
              LOG.warn("Attempt to send metric to overlord, " + "failed " + "due to session failures. [ {} ]", cmd);
            } else {
              LOG.debug("Suceesfully sent metric {} to overlord.", cmd);
            }
          }
        });
      }
    }
    // Dispose the connector.
    if (connector != null) {
      connector.dispose();
      connector = null;
    }

    // Close the session.
    if (session != null) {
      session.close(true).awaitUninterruptibly(CONNECT_TIMEOUT);
      session = null;
    }

    LOG.debug("Mensa metrics dispatcher finished!");
  }
  private boolean connect() {
    // If we have a session and it's connected to the overlord metrics
    // server, then we return true immediately, else we try connecting
    // to the overlord metrics server.
    if (session != null && session.isConnected()) {
      return true;
    }

    // Connect to the server.
    LOG.info("Connecting to service endpoint {}:{}.", hostname, port);
    ConnectFuture cf = connector.connect(new InetSocketAddress(hostname, port));

    // Wait till we are connected or 10 seconds are done.
    cf.awaitUninterruptibly();

    // Check if we are connected.
    if (cf.isConnected()) {
      LOG.info("Successfully connected to endpoint {}:{}", hostname, port);
      session = cf.getSession();
      return true;
    } else {
      LOG.warn("Unable to connect to endpoint {}:{}", hostname, port);
    }

    return false;
  }
}
