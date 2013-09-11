package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.Copyright;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.stubs.TOperationExecutor;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
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
public class NoOpexService {

  private static final Logger LOG =
      LoggerFactory.getLogger(NoOpexService.class);

  /* the port to run on */
  int port;

  /* the internet address to run on */
  String address;

  /* the number of threads to use for the thrift service */
  int threads;

  /* the thrift service */
  THsHaServer server;

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
  public NoOpexService(OperationExecutor opex) {
    this.opex = opex;
  }

  public void start(CConfiguration conf) {

    try {
      // Retrieve the port and the number of threads for the service
      this.port = conf.getInt(Constants.CFG_DATA_OPEX_SERVER_PORT,
          Constants.DEFAULT_DATA_OPEX_SERVER_PORT);
      this.address = conf.get(Constants.CFG_DATA_OPEX_SERVER_ADDRESS,
        Constants.DEFAULT_DATA_OPEX_SERVER_ADDRESS);
      this.threads = conf.getInt(Constants.CFG_DATA_OPEX_SERVER_THREADS,
          Constants.DEFAULT_DATA_OPEX_SERVER_THREADS);

      InetSocketAddress socketAddr = new InetSocketAddress(address, port);

      LOG.info("Configuring Operation Executor Service: " + this.threads +
                 " threads at " + socketAddr.toString());

      // create a new thread pool
      this.executorService = Executors.newCachedThreadPool();

      // configure a thrift service
      THsHaServer.Args serverArgs =
          new THsHaServer.Args(new TNonblockingServerSocket(socketAddr))
              .executorService(executorService)
              .processor(new TOperationExecutor.
                  Processor<TOperationExecutor.Iface>(
                      new TOperationExecutorImpl(this.opex)))
              .workerThreads(20);

      // ENG-443 - Set the max read buffer size. This is important as this will
      // prevent the server from throwing OOME if telnetd to the port
      // it's running on.
      serverArgs.maxReadBufferBytes = com.continuuity.common.conf.Constants.Thrift.DEFAULT_MAX_READ_BUFFER;

      this.server = new THsHaServer(serverArgs);

      // serve() blocks and will only return when stop() is called
      this.server.serve();

    } catch (TTransportException e) {
      LOG.error("Failed to create THsHa server for Operation Executor " +
                  "Service. Reason : {}", e.getMessage());
      this.stop();
    }
  }

  protected void stop() {
    LOG.info("Stopping Operation Executor Service on port " +
               NoOpexService.this.port);
    if (this.server != null) {
      this.server.stop();
      this.server = null;
    }
    if (this.executorService != null) {
      this.executorService.shutdown();
      this.executorService = null;
    }
    LOG.info("Operation Executor Service on port " +
               NoOpexService.this.port + " is stopped");
  }

  static void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    String name = NoOpexService.class.getSimpleName();
    Copyright.print(out);
    out.println("Usage: ");
    out.println("  " + name + " ( start | stop ) ");
  }

  public static void main(String[] args) throws IOException, InterruptedException {

    CConfiguration configuration = CConfiguration.create();
    NoOpexService opexService = new NoOpexService(new NoOperationExecutor());

    System.out.println("Starting Operation Executor Service...");
    // start it. start is blocking, hence main won't terminate
    try {
      opexService.start(configuration);
    } catch (Exception e) {
      System.err.println("Failed to start service: " + e.getMessage());
      return;
    }
  }

}
