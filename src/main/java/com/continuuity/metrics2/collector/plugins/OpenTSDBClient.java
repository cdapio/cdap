package com.continuuity.metrics2.collector.plugins;

import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.future.CloseFuture;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;

/**
 * Async client for forwarding requests of type {@link com.continuuity
 * .metrics.MetricType.System} to OpenTSDB system.
 */
final class OpenTSDBClient extends IoHandlerAdapter {
  private static final Logger Log
    = LoggerFactory.getLogger(OpenTSDBClient.class);

  /**
   * Connection timeout.
   */
  public static final long CONNECT_TIMEOUT = 30 *1000L;

  /**
   * Connect retry attempts.
   */
  public static final int RETRY_ATTEMPTS = 50;

  /**
   * Intra connect gap (sleep).
   */
  public static final long INTRA_CONNECT_SLEEP_MS = 500 * 1000L;

  /**
   * Hostname of OpenTSDB.
   */
  private final String hostname;

  /**
   * Port for OpenTSDB.
   */
  private final int port;

  /**
   * TCP connector used for connecting to opentsdb.
   */
  private NioSocketConnector connector;

  /**
   * Queue of sessions
   */
  private IoSession session;


  public OpenTSDBClient(final String hostname, final int port) {
    this.hostname = hostname;
    this.port = port;
    connector = new NioSocketConnector();
    connector.setConnectTimeoutMillis(CONNECT_TIMEOUT);

    // Add a codec to the filter list.
    connector.getFilterChain().addLast("codec", new ProtocolCodecFilter(
      new TextLineCodecFactory(Charset.defaultCharset())
    ));
    connector.setHandler(this);

    // Add a shutdown hook so that we terminate the sessions
    // gracefully.
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        if(session != null) {
          session.close(true).addListener(new IoFutureListener<CloseFuture>() {
            @Override
            public void operationComplete(CloseFuture future) {
              if(future.isClosed()) {
                Log.trace("Successfully close session.");
              }
            }
          });
        }
        if(connector != null) {
          connector.dispose();
        }
        session = null;
        connector = null;
      }
    }));
  }

  /**
   * Provides a new session or selects from one of the available session.
   *
   * @return Instance of session or null if cannot find or create one.
   */
  private IoSession connect() {

    // if there idle sessions available then we take from it
    // else we proceed further to create a session to be used.
    if(session != null && session.isConnected()) {
      return session;
    }

    // if we are here means that we have not found a session
    // available that is idle, so we go a head and create one.
    // once, the session is connected it's session opened
    // handler will add it to the idle list.
    Log.info("Connecting to OpenTSDB server host {} on port {}.",
      hostname, port);
    ConnectFuture connectFuture =
      connector.connect(new InetSocketAddress(hostname, port));
    connectFuture.awaitUninterruptibly();

    try {
      return connectFuture.getSession();
    } catch (RuntimeIoException e) {
      Log.warn("Failing to connect to opentsdb server. Reason : {}",
               e.getMessage());
    }

    return null;
  }

  /**
   * Sends the metric request to openTSDB.
   * <p>
   *   If connection is not established, then it attempts to connect
   *   N times before bailing out.
   * </p>
   *
   * @param request
   * @return
   * @throws IOException
   */
  public WriteFuture send(String request) throws IOException {
    session = connect();
    if(session == null) {
      int attempts = RETRY_ATTEMPTS;
      while(attempts > 0) {
        try {
          Thread.sleep(INTRA_CONNECT_SLEEP_MS);
        } catch (InterruptedException e) {}
        attempts--;
        session = connect();
      }
      if(attempts < 1) {
        throw new IOException("Failed connecting to opentsdb " +
                                "after multiple attempts.");
      }
    }
    return session.write(request);
  }

  /**
   * Callback when there are issues with the session.
   *
   * <p>
   *   The problematic session is removed from the session list.
   * </p>
   *
   * @param session being processed.
   * @param cause of failure.
   * @throws Exception
   */
  @Override
  public void exceptionCaught(IoSession session, Throwable cause) throws
    Exception {
    // We proactively remove the session that is a problem.
    Log.warn(cause.getMessage(), cause);
  }

}
