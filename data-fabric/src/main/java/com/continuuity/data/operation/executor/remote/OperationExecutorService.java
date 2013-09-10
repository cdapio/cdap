package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.service.AbstractRegisteredServer;
import com.continuuity.common.service.RegisteredServerInfo;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.stubs.TOperationExecutor;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This implements an Operation Executor as a Thrift service. It registers
 * itself with the service discovery under the name "opex-service" (@see
 * SERVICE_NAME).
 *
 * Configuration:
 * <ul><li>
 *   data.opex.server.port (@see Constants.CFG_DATA_OPEX_SERVER_PORT) for the
 *   port number to bind the server to. Default is 15165.
 * </li><li>
 *   data.opex.server.threads (@see Constants.CFG_DATA_OPEX_SERVER_THREADS) for
 *   the number of worker threads to start. Default is 20.
 * </li></ul>
 *
 * The thrift service is not meant to be called directly, but through an
 * instance of @link RemoteOperationExecutor.
 */
public class OperationExecutorService extends AbstractRegisteredServer {

  private static final Logger Log =
      LoggerFactory.getLogger(OperationExecutorService.class);

  /* the port to run on */
  int port;

  /* the internet address to run on */
  String address;

  /* the number of IO threads to use for the thrift service */
  int ioThreads;

  /* the number of threads to use for the thrift service */
  int threads;

  /* the thrift service */
  TServer server;

  /* the operation executor for the actual data fabric */
  OperationExecutor opex;

  /* a pool of threads for the thrift service */
  ExecutorService executorService;

  /**
   * All this does is set the name of the service so that it can
   * be uniquely identified by the service discovery client. The
   * name for this service is "opex-service".
   * @param opex the operation executor to use.
   */
  @Inject
  public OperationExecutorService(
      @Named("DataFabricOperationExecutor")OperationExecutor opex) {
    super.setServerName(Constants.OPERATION_EXECUTOR_SERVICE_NAME);
    this.opex = opex;
  }

  @Override
  protected RegisteredServerInfo configure(String[] args, CConfiguration conf) {

    try {
      // Retrieve the port and the number of threads for the service
      this.port = conf.getInt(Constants.CFG_DATA_OPEX_SERVER_PORT,
          Constants.DEFAULT_DATA_OPEX_SERVER_PORT);
      this.address = conf.get(Constants.CFG_DATA_OPEX_SERVER_ADDRESS,
          Constants.DEFAULT_DATA_OPEX_SERVER_ADDRESS);
      this.ioThreads = conf.getInt(Constants.CFG_DATA_OPEX_SERVER_IO_THREADS,
          Constants.DEFAULT_DATA_OPEX_SERVER_IO_THREADS);
      this.threads = conf.getInt(Constants.CFG_DATA_OPEX_SERVER_THREADS,
          Constants.DEFAULT_DATA_OPEX_SERVER_THREADS);

      InetSocketAddress socketAddr = new InetSocketAddress(address, port);

      Log.info("Configuring Operation Executor Service: " + this.threads +
          " threads at " + socketAddr.toString());

      // create a new thread pool
      this.executorService = Executors.newFixedThreadPool(threads,
                                                          new ThreadFactoryBuilder()
                                                            .setNameFormat("opex-thrift-%d")
                                                            .build());

      TOperationExecutor.Processor<TOperationExecutor.Iface> processor = new TOperationExecutor.
        Processor<TOperationExecutor.Iface>(
        new TOperationExecutorImpl(this.opex));
      // configure a thrift service

      TThreadedSelectorServer.Args serverArgs =
        new TThreadedSelectorServer.Args(new TNonblockingServerSocket(socketAddr))
          .selectorThreads(ioThreads)
          .transportFactory(new TFramedTransport.Factory())
          .processor(processor)
          .executorService(executorService);


      // ENG-443 - Set the max read buffer size. This is important as this will
      // prevent the server from throwing OOME if telnetd to the port
      // it's running on.
      serverArgs.maxReadBufferBytes = getMaxReadBuffer(conf);

      this.server = new TThreadedSelectorServer(serverArgs);

      // and done, return the payload
      RegisteredServerInfo info = new RegisteredServerInfo(address, port);
      info.addPayload("thread", Integer.toString(this.threads));
      return info;
    } catch (TTransportException e) {
      Log.error("Failed to create THsHa server for Operation Executor " +
          "Service. Reason : {}", e.getMessage());
      this.stop();
    }
    return null;
  }

  @Override
  protected Thread start() {
    return new Thread() {
      public void run() {
        Log.info("Starting Operation Executor Service on port " +
            OperationExecutorService.this.port);
        // serve() blocks and will only return when stop() is called
        OperationExecutorService.this.server.serve();
      }
    };
  }

  @Override
  protected void stop() {
    Log.info("Stopping Operation Executor Service on port " +
        OperationExecutorService.this.port);
    if (this.server != null) {
      this.server.stop();
      this.server = null;
    }
    if (this.executorService != null) {
      this.executorService.shutdown();
      this.executorService = null;
    }
    Log.info("Operation Executor Service on port " +
        OperationExecutorService.this.port + " is stopped");
  }

  @Override
  protected boolean ruok() {
    // server may be null if this is called before configure(),
    // or if configure() failed, or if called after stop().
    return (this.server != null) && (this.server.isServing());
  }
}
