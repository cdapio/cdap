package com.continuuity.internal.app.services;

import com.continuuity.app.program.Store;
import com.continuuity.app.services.ProgramManagerServer;
import com.continuuity.app.services.ProgramService;
import com.continuuity.app.services.ProgramServiceHandler;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.ServiceDiscoveryClient;
import com.continuuity.common.discovery.ServiceDiscoveryClientException;
import com.continuuity.common.metrics.OverlordMetricsReporter;
import com.continuuity.common.service.RegisteredServerInfo;
import com.continuuity.common.service.ServerException;
import com.google.inject.Inject;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of ProgramManagerServer as Thrift server
 */
public final class ProgramManagerThriftServer implements ProgramManagerServer {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramManagerThriftServer.class);

  /**
   * Manages threads.
   */
  private ExecutorService executorService;

  /**
   * Program Service handler.
   */
  private ProgramServiceHandler serviceHandler;

  /**
   * Instance of Program Store
   */
  private final Store store;

  /**
   * Half-Sync, Half-Async Thrift server.
   */
  private THsHaServer server;

  @Inject
  public ProgramManagerThriftServer(Store store) {
    this.store = store;
  }

  /**
   * Starts the {@link com.continuuity.common.service.Server}
   *
   * @param args arguments for the service
   * @param conf instance of configuration object.
   */
  @Override
  public void start(String[] args, CConfiguration conf) throws ServerException {
    String zkEnsemble = conf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE, Constants.DEFAULT_ZOOKEEPER_ENSEMBLE);

    try {
      executorService = Executors.newCachedThreadPool();

      this.serviceHandler = new ProgramServiceHandlerImpl(store);

      int port = conf.getInt(Constants.CFG_FLOW_MANAGER_SERVER_PORT,
        Constants.DEFAULT_FLOW_MANAGER_SERVER_PORT);

      int threads = conf.getInt(Constants.CFG_FLOW_MANAGER_SERVER_THREADS,
        Constants.DEFAULT_FLOW_MANAGER_SERVER_THREADS);

      ProgramServiceImpl serviceImpl = new ProgramServiceImpl(serviceHandler);
      THsHaServer.Args serverArgs =
        new THsHaServer
          .Args(new TNonblockingServerSocket(port))
          .executorService(executorService)
          .processor(new ProgramService.Processor(serviceImpl))
          .workerThreads(threads);

      OverlordMetricsReporter.enable(1, TimeUnit.SECONDS, conf);

      // ENG-443 - Set the max read buffer size. This is important as this will
      // prevent the server from throwing OOME if telnetd to the port
      // it's running on.
      serverArgs.maxReadBufferBytes = Constants.DEFAULT_MAX_READ_BUFFER;

      server = new THsHaServer(serverArgs);
      LOG.info("Starting program service on port {}", port);
      new Thread ( new Runnable() {
        @Override
        public void run() {
          server.serve();
        }
      }).start();

      try {
        // Provide the registration info of service.
        RegisteredServerInfo info
            = new RegisteredServerInfo("localhost", port);
        info.addPayload("threads", Integer.toString(threads));
        ServiceDiscoveryClient client = new ServiceDiscoveryClient(zkEnsemble);
        client.register(Constants.SERVICE_FLOW_SERVER,
            info.getAddress(), info.getPort(), info.getPayload());
      } catch (ServiceDiscoveryClientException e) {
        String message = "Error registering program service with service " +
            "discovery: " + e.getMessage();
        LOG.error(message);
        throw new ServerException(message, e);
      }

    } catch (TTransportException e) {
      LOG.error("Non-blocking server error. Reason : {}", e.getMessage());
    }
  }

  /**
   * Stops the {@link com.continuuity.common.service.Server}
   *
   * @param now true specifies non-graceful shutdown; false otherwise.
   */
  @Override
  public void stop(boolean now) throws ServerException {
    if(server != null) {
      server.stop();
    }
  }
}
