package com.continuuity.common.metrics;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.ServiceDiscoveryClient;
import com.continuuity.common.discovery.ServiceDiscoveryClientException;
import com.continuuity.common.discovery.ServicePayload;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.common.metrics.codec.MetricCodecFactory;
import com.google.common.base.Preconditions;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.strategies.RandomStrategy;
import org.apache.commons.lang.time.StopWatch;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * This client is similar to the NetCat client. It connects
 * to the specified end point and sends the command to be executed
 * on the server.
 */
public class MetricsClient {
  private static final Logger Log =
    LoggerFactory.getLogger(MetricsClient.class);

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
  private String hostname;

  /**
   * Port to connect to.
   */
  private int port;

  /**
   * Session associated with the current connection.
   */
  private IoSession session = null;

  /**
   * instance of dispatcher responsible for sending metrics
   * to the overlord server.
   */
  private final MetricsDispatcher dispatcher =
    new MetricsDispatcher();

  /**
   * Queue that
   */
  private final LinkedBlockingDeque<String> queue;

  /**
   * TCP connector.
   */
  private NioSocketConnector connector;

  /**
   * Executor service for running the dispatcher thread.
   * Overkill right now -- the idea is that in future if
   * we find that we need to more threads to send then
   * it can be extended.
   */
  private final ExecutorService executorService =
    Executors.newCachedThreadPool();

  /**
   * Client used for discoverying the metrics collector service.
   */
  private final ServiceDiscoveryClient serviceDiscovery;

  /**
   * Dispatcher for handling sending metrics to the overlord server.
   * It runs in a seperate thread dequeuing the commands written by
   * the client metric collector. If it's unable to connect to the
   * server, it implements a exponential backoff to make sure we don't
   * tax the server trying to connect.
   */
  private class MetricsDispatcher implements Runnable {
    private volatile boolean keepRunning = true;
    private final StopWatch watcher = new StopWatch();

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
      while(keepRunning) {
        // Try to get a session while session is not created
        // or if created and is not connected.
        while(session == null || !session.isConnected()){
          try {
            // Make an attempt to connect, it does so by getting the
            // latest endpoint from service disocvery and tries to
            // connect to it.
            connect();

            if(session == null || (session != null && ! session.isConnected())){
              // Sleep based on how much ever is interval set to.
              try {
                Log.warn("Backing off after unable to connect to metrics " +
                           "collector host {}:{} for {}s.",
                         new Object[] {hostname, port, interval});
                Thread.sleep(interval * 1000L);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break; // Go back to check if we have asked to stop.
              }

              // Exponentially increase the amount of time to sleep,
              // untill we reach 30 seconds sleep between reconnects.
              interval = Math.min(BACKOFF_MAX_TIME, interval*BACKOFF_EXPONENT);
            } else {
              // we are conected and now need to send data.
              break;
            }
            // If we are here means
          } catch (ServiceDiscoveryClientException e) {
            Log.warn("Issue with service discovery. Reason : {}.",
              e.getMessage());
            Log.debug(StackTraceUtil.toStringStackTrace(e));
          }
        }

        // We pop the metric to be send from the queue and
        // then attempt to send it over. If we fail and there
        // is space available we write the metric back into
        // the queue.
        final String cmd;
        String element = null;
        try {
          // blocking call will wait till there is an element in the queue.
          element = queue.take();
        } catch (InterruptedException e) {
          Log.warn("Thread has been interrupted.");
          continue;
        }

        // Make sure we have not received a null object. This is
        // just a precaution.
        if(element == null) {
          continue;
        }
        cmd = element;

        // Write the command to the session and attach a future for reporting
        // any issues seen.
        WriteFuture future = session.write(cmd);
        if(future != null) {
          future.addListener(new IoFutureListener<WriteFuture>() {
            @Override
            public void operationComplete(WriteFuture future) {
              if(! future.isWritten()) {
                Log.warn("Attempted to send metric to overlord, " +
                           "failed " + "due to session failures. [ {} ]", cmd);
              }
            }
          });
        }
      }
    }
  }

  /**
   * Constructs and initializes {@link MetricsClient}.
   *
   * @param configuration object.
   * @throws ServiceDiscoveryClientException thrown when the client is
   * unable to discovery the service or unable to connect to zookeeper.
   */
  public MetricsClient(CConfiguration configuration)
    throws ServiceDiscoveryClientException {

    // Creates the queue that holds the metrics to be dispatched
    // to the overlord.
    this.queue = new LinkedBlockingDeque<String>(50000);

    // Prepare the connection.
    connector = new NioSocketConnector();

    // Sets aggressive connection timeout.
    connector.setConnectTimeoutMillis(CONNECT_TIMEOUT);

    // add an IoFilter .  This class is responsible for converting the incoming and
    // outgoing raw data to MetricRequest and MetricResponse objects
    ProtocolCodecFilter protocolFilter
      = new ProtocolCodecFilter(new MetricCodecFactory(true));

    // Set the protocol filter to metric codec factory.
    connector.getFilterChain().addLast("protocol", protocolFilter);

    // Set Keep Alive.
    connector.getSessionConfig().setKeepAlive(true);

    // Set to send packets of any size. As our requests are small,
    // we don't want them to be batched.
    connector.getSessionConfig().setTcpNoDelay(true);

    // Attach a handler.
    connector.setHandler(new MetricsClientProtocolHandler());

    // Register a shutdown hook.
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        // Shutdown the dispatcher thread.
        if(dispatcher != null) {
          dispatcher.stop();
        }

        // Dispose the connector.
        if(connector != null) {
          connector.dispose();
          connector = null;
        }

        // Shutdown executor service.
        executorService.shutdown();

        // Close service discovery
        if(serviceDiscovery != null) {
          try {
            serviceDiscovery.close();
          } catch (IOException e) {
            Log.warn("Failed closing service discovery client. Reason : {}.",
              e.getMessage());
            Log.debug(StackTraceUtil.toStringStackTrace(e));
          }
        }

        // Close the session.
        if(session != null) {
          session.close(true).awaitUninterruptibly(CONNECT_TIMEOUT);
          session = null;
        }
      }
    });

    // prepare service discovery client.
    serviceDiscovery = new ServiceDiscoveryClient(
      configuration.get(Constants.CFG_ZOOKEEPER_ENSEMBLE,
                        Constants.DEFAULT_ZOOKEEPER_ENSEMBLE)
    );


    // Start the dispatcher thread.
    executorService.submit(dispatcher);
  }

  /**
   * Discovers the endpoint using the service discovery.
   */
  private void getServiceEndpoint() throws ServiceDiscoveryClientException {
    ServiceInstance<ServicePayload> instance =
          serviceDiscovery.getInstance(Constants.SERVICE_METRICS_COLLECTION_SERVER,
                                       new RandomStrategy<ServicePayload>());
    this.hostname = instance.getAddress();
    this.port = instance.getPort();
    Log.info("Received service endpoint {}:{}.", this.hostname, this.port);
  }

  private boolean connect() throws ServiceDiscoveryClientException {
    // On every reconnect attempt try to get the new service end point.
    // If there are multiple instance of the service available then we
    // would be able to connect to atleast one.
    getServiceEndpoint();

    // If we have a session and it's connected to the overlord metrics
    // server, then we return true immediately, else we try connecting
    // to the overlord metrics server.
    if(session != null && session.isConnected()) {
      return true;
    }

    // Connect to the server.
    Log.info("Connecting to service endpoint {}:{}.", hostname, port);
    ConnectFuture cf = connector.connect(
      new InetSocketAddress(hostname, port)
    );

    // Wait till we are connected or 10 seconds are done.
    cf.awaitUninterruptibly();

    // Check if we are connected.
    if(cf.isConnected()) {
      Log.info("Successfully connected to endpoint {}:{}", hostname, port);
      session = cf.getSession();
      return true;
    } else {
      Log.warn("Unable to connect to endpoint {}:{}", hostname, port);
    }

    return false;
  }

  /**
   * Writes the metric to be sent to queue.
   *
   * @param buffer contains the command to be sent to the server.
   * @return true if successfully put on the queue else false.
   */
  public boolean write(String buffer) {
    Preconditions.checkNotNull(buffer);
    return queue.offer(buffer);
  }

}
