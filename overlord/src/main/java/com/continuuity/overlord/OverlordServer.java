package com.continuuity.overlord;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.service.AbstractRegisteredServer;
import com.continuuity.common.service.RegisteredServerInfo;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.metadata.MetadataService;
import com.continuuity.weave.common.Threads;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Overlord consolidated server.
 */
public class OverlordServer extends AbstractRegisteredServer implements
  OverlordServerInterface {
  private static final Logger Log = LoggerFactory.getLogger(
    OverlordServer.class
  );

  /**
   * Instance of operation executor
   */
  private final OperationExecutor opex;

  /**
   * Manages threads.
   */
  private ExecutorService executorService
    = Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("overlord-server-%d"));

  /**
   * Half-Sync, Half-Async Thrift server.
   */
  private THsHaServer server;

  public OverlordServer(OperationExecutor opex) {
    this.opex = opex;
  }

  /**
   * Extending class should implement this API. Should do everything to start
   * a service in the Thread.
   *
   * @return Thread instance that will be managed by the service.
   */
  @Override
  protected Thread start() {
    return new Thread(new Runnable() {
      @Override
      public void run() {
        server.serve();
      }
    });
  }

  /**
   * Should be implemented by the class extending {@link com.continuuity
   * .common.service.AbstractRegisteredServer} to stop the service.
   */
  @Override
  protected void stop() {
    if (server != null) {
      server.stop();
    }
    executorService.shutdown();
  }

  /**
   * Override to investigate the service and return status.
   *
   * @return true if service is running and good; false otherwise.
   */
  @Override
  protected boolean ruok() {
    return server.isServing();
  }

  /**
   * Configures the service.
   *
   * @param args from command line based for configuring service
   * @param conf Configuration instance passed around.
   * @return Pair of args for registering the service and the port service is running on.
   */
  @Override
  protected RegisteredServerInfo configure(String[] args, CConfiguration conf) {
    try {
      // Get the port the server should run on.
      InetAddress serverAddress = getServerInetAddress(conf.get(
        Constants.CFG_OVERLORD_SERVER_ADDRESS
      ));

      // Get the port that the server should be started on from configuration.
      int serverPort = conf.getInt(
        Constants.CFG_OVERLORD_SERVER_PORT,
        Constants.DEFAULT_OVERLORD_SERVER_PORT
      );

      // Number of threads on the server to handle clients.
      int threads = conf.getInt(
        Constants.CFG_OVERLORD_SERVER_THREADS,
        Constants.DEFAULT_OVERLORD_SERVER_THREADS
      );

      // Attach and handler to the metadata service thrift interface.
      MetadataService service
        = new MetadataService(opex);

      // configure the server
      THsHaServer.Args serverArgs =
        new THsHaServer
          .Args(new TNonblockingServerSocket(new InetSocketAddress(
          serverAddress,
          serverPort
        )))
          .executorService(executorService)
          .processor(new com.continuuity.metadata.thrift.MetadataService.Processor(service))
          .workerThreads(threads);

      // ENG-443 - Set the max read buffer size. This is important as this will
      // prevent the server from throwing OOME if telnetd to the port
      // it's running on.
      serverArgs.maxReadBufferBytes = getMaxReadBuffer(conf);

      // create a new Half-Sync / Half-Async server.
      server = new THsHaServer(serverArgs);

      // Set the server name.
      setServerName(Constants.SERVICE_METADATA_SERVER);

      // Provide the registration info of service.
      RegisteredServerInfo info
        = new RegisteredServerInfo(serverAddress.getHostName(), serverPort);
      info.addPayload("threads", Integer.toString(threads));
      Log.info("Server starting on {}:{}", serverAddress.getHostAddress(),
               serverPort);
      return info;
    } catch (UnknownHostException e) {
      Log.error("Starting server on unknown host. Reason : {}", e.getMessage());
      stop();
    } catch (TTransportException e) {
      Log.error("Non-blocking server error. Reason : {}", e.getMessage());
      stop();
    }
    return null;
  }
}
