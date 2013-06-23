package com.continuuity.metrics2.frontend;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.service.AbstractRegisteredServer;
import com.continuuity.common.service.RegisteredServerInfo;
import com.continuuity.metrics2.thrift.MetricsFrontendService;
import com.continuuity.weave.common.Threads;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Metrics frontend server class that is injectable.
 */
public class MetricsFrontendServer extends AbstractRegisteredServer
      implements MetricsFrontendServerInterface {

  private static final Logger Log = LoggerFactory.getLogger(MetricsFrontendServer.class);
  private static final int MAX_THREAD_POOL_SIZE = 50;

  /**
   * Manages threads.
   *
   * Thread pool of size max MAX_THREAD_POOL_SIZE.
   * 60 seconds wait time before killing idle threads.
   * Keep no idle threads more than 60 seconds.
   * If max thread pool size reached, reject the new coming
   */
  private final ExecutorService executorService =
    new ThreadPoolExecutor(0, MAX_THREAD_POOL_SIZE,
                           60L, TimeUnit.SECONDS,
                           new SynchronousQueue<Runnable>(),
                           Threads.createDaemonThreadFactory("metrics-frontend-%d"),
                           new ThreadPoolExecutor.DiscardPolicy());

  /**
   * Half-Sync, Half-Async Thrift server.
   */
  private THsHaServer server;

  /**
   * @return an instance of thread within which the server would run blocked.
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
   * Stops the server.
   */
  @Override
  protected void stop() {
    if(server != null) {
      server.stop();
    }
    executorService.shutdown();
  }

  /**
   * @return true if server is ok; false otherwise.
   */
  @Override
  protected boolean ruok() {
    return server.isServing();
  }

  /**
   * Configures the server.
   *
   * @param args from command line based for configuring service
   * @param conf Configuration instance passed around.
   * @return registration information of the server for service discovery.
   */
  @Override
  protected RegisteredServerInfo configure(String[] args, CConfiguration conf){
    try {
      // Get the port the server should run on.
      InetAddress serverAddress = getServerInetAddress(conf.get(
        Constants.CFG_METRICS_FRONTEND_SERVER_ADDRESS
      ));

      // Get the port that the server should be started on from configuration.
      int serverPort = conf.getInt(
                          Constants.CFG_METRICS_FRONTEND_SERVER_PORT,
                          Constants.DEFAULT_METRICS_FRONTEND_SERVER_PORT
      );

      // Number of threads on the server to handle clients.
      int threads = conf.getInt(
                          Constants.CFG_METRICS_FRONTEND_SERVER_THREADS,
                          Constants.DEFAULT_METRICS_FRONTEND_SERVER_THREADS
      );

      // Attach and handler to the metrics frontend service thrift interface.
      MetricsFrontendServiceImpl serviceImpl
        = new MetricsFrontendServiceImpl(conf);

      // configure the server
      THsHaServer.Args serverArgs =
        new THsHaServer
          .Args(new
                TNonblockingServerSocket(new InetSocketAddress(
                                                  serverAddress,
                                                  serverPort
                )))
          .executorService(executorService)
          .processor(new MetricsFrontendService.Processor(serviceImpl))
          .workerThreads(threads);

      // ENG-443 - Set the max read buffer size. This is important as this will
      // prevent the server from throwing OOME if telnetd to the port
      // it's running on.
      serverArgs.maxReadBufferBytes = getMaxReadBuffer(conf);

      // create a new Half-Sync / Half-Async server.
      server = new THsHaServer(serverArgs);

      // Set the server name.
      setServerName(Constants.SERVICE_METRICS_FRONTEND_SERVER);

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
    } catch (ClassNotFoundException e) {
      Log.error("Server failed to load appropriate SQL driver. Reason : {}",
                e.getMessage());
      stop();
    } catch (SQLException e) {
      Log.error("Server failed initializing database connection. Reason : {}",
                e.getMessage());
      stop();
    }
    return null;
  }
}
