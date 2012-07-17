package com.continuuity.metrics.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.ServiceDiscoveryClient;
import com.continuuity.common.service.AbstractRegisteredServer;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.metrics.stubs.FlowMonitor;
import com.continuuity.observer.StateChangeCallback;
import com.continuuity.observer.StateChangeListener;
import com.continuuity.observer.StateChangeListenerException;
import com.continuuity.observer.internal.StateChange;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryUntilElapsed;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 *
 */
public class MetricsRegisteredServer extends AbstractRegisteredServer implements MetricsServer {
  private static final Logger Log = LoggerFactory.getLogger(MetricsRegisteredServer.class);
  private static final String STATE_CHANGE_QUEUE = "/continuuity/system/queue/statechange";
  private THsHaServer server;
  private CuratorFramework client;
  private ExecutorService executorService;
  private final MetricsHandler handler;
  private final StateChangeCallback callback;
  private StateChangeListener listener;

  @Inject
  public MetricsRegisteredServer(MetricsHandler handler, StateChangeCallback callback) {
    setServerName("flow-monitor");
    this.handler = handler;
    this.callback = callback;
  }

  /**
   * Extending class should implement this API. Should do everything to start a service in the Thread.
   *
   * @return Thread instance that will be managed by the service.
   */
  @Override
  protected Thread start() {
    Log.info("Started Flow Monitor registered service ...");
    return new Thread(new Runnable() {
      @Override
      public void run() {
        server.serve();
      }
    });
  }

  /**
   * Should be implemented by the class extending {@link com.continuuity.common.service.AbstractRegisteredServer} to stop the service.
   */
  @Override
  protected void stop() {
    Closeables.closeQuietly(client);
    server.stop();
    executorService.shutdown();
  }

  /**
   * Override to investigate the service and return status.
   *
   * @return true if service is running and good; false otherwise.
   */
  @Override
  protected boolean ruok() {
    return server.isServing() && !server.isStopped();
  }

  /**
   * Configures the service.
   *
   * @param args from command line based for configuring service
   * @param conf Configuration instance passed around.
   * @return Pair of args for registering the service and the port service is running on.
   */
  @Override
  protected ImmutablePair<ServiceDiscoveryClient.ServicePayload, Integer> configure(String[] args, CConfiguration conf) {
    String uri = conf.get("overlord.jdbc.uri", null);

    try {
      handler.init(uri);
      callback.init(uri);
    } catch (Exception e) {
      Log.error("Failed to initialize metrics handler. Reason : {}", e.getMessage());
      return null;
    }

    String zkEnsemble = conf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE);
    Log.info("Connecting to zookeeper {}", zkEnsemble);
    try {
      executorService = Executors.newCachedThreadPool();
      client = CuratorFrameworkFactory.newClient(zkEnsemble, new RetryUntilElapsed(30000, 2000));
      client.start();

      listener = StateChange.Server.newListener(client);
      listener.listen(STATE_CHANGE_QUEUE, callback);

      String portProperty = conf.get(Constants.CFG_FLOW_MONITOR_SERVER_PORT,
        Constants.DEFAULT_FLOW_MONITOR_SERVER_PORT);
      int port = Integer.valueOf(portProperty);

      String threadCntProperty = conf.get(Constants.CFG_FLOW_MONITOR_SERVER_THREADS,
        Constants.DEFAULT_FLOW_MONITOR_SERVER_THREADS);
      int threads = Integer.valueOf(threadCntProperty);

      MetricsImpl serviceImpl = new MetricsImpl(handler);
      THsHaServer.Args serverArgs =
        new THsHaServer
          .Args(new TNonblockingServerSocket(port))
          .executorService(executorService)
          .processor(new FlowMonitor.Processor(serviceImpl))
          .workerThreads(threads);
      server = new THsHaServer(serverArgs);
      ServiceDiscoveryClient.ServicePayload payload =
        new ServiceDiscoveryClient.ServicePayload();
      payload.add("threads", threadCntProperty);
      return new ImmutablePair<ServiceDiscoveryClient.ServicePayload, Integer>(payload, port);
    } catch (IOException e) {
      Log.error("Failed to create FARService server. Reason : {}", e.getMessage());
      stop();
    } catch (TTransportException e) {
      Log.error("Non-blocking server error. Reason : {}", e.getMessage());
    } catch (StateChangeListenerException e) {
      Log.error("Error listening to state change queue.");
    }
    return null;
  }

}
