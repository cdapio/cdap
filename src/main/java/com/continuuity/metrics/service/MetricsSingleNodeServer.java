package com.continuuity.metrics.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.service.ServerException;
import com.continuuity.metrics.stubs.FlowMonitor;
import com.continuuity.observer.StateChangeCallback;
import com.continuuity.observer.StateChangeListener;
import com.continuuity.observer.StateChangeListenerException;
import com.continuuity.observer.internal.StateChange;
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
 */
public class MetricsSingleNodeServer implements MetricsServer {
  private static final Logger Log = LoggerFactory.getLogger(MetricsSingleNodeServer.class);
  private static final String STATE_CHANGE_QUEUE = "/continuuity/system/queue/statechange";
  private THsHaServer server;
  private CuratorFramework client;
  private ExecutorService executorService;
  private final MetricsHandler handler;
  private final StateChangeCallback callback;
  private StateChangeListener listener;
  private static final String DEFAULT_JDBC_URI="jdbc:hsqldb:mem:fmdb";

  @Inject
  public MetricsSingleNodeServer(MetricsHandler handler, StateChangeCallback callback) {
    this.handler = handler;
    this.callback = callback;
  }

  /**
   * Starts the {@link com.continuuity.common.service.Server}
   *
   * @param args arguments for the service
   * @param conf instance of configuration object.
   */
  @Override
  public void start(String[] args, CConfiguration conf) throws ServerException {
    String uri = conf.get("overlord.jdbc.uri", DEFAULT_JDBC_URI);

    try {
      handler.init(uri);
      callback.init(uri);
    } catch (Exception e) {
      throw new ServerException(e.getMessage());
    }

    String zkEnsemble = conf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE, Constants.DEFAULT_ZOOKEEPER_ENSEMBLE);
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
      new Thread(new Runnable() {
        @Override
        public void run() {
          server.serve();
        }
      }).start();
    } catch (IOException e) {
      Log.error("Failed to create FARService server. Reason : {}", e.getMessage());
      throw new ServerException("Failed to create FARService server", e);
    } catch (TTransportException e) {
      Log.error("Non-blocking server error. Reason : {}", e.getMessage());
      throw new ServerException("Non-blocking server error.", e);
    } catch (StateChangeListenerException e) {
      Log.error("Error listening to state change queue.");
      throw new ServerException("Error listening to state change queue.", e);
    }
  }

  /**
   * Stops the {@link com.continuuity.common.service.Server}
   *
   * @param now true specifies non-graceful shutdown; false otherwise.
   */
  @Override
  public void stop(boolean now) throws ServerException {

  }

}
