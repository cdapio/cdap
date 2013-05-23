package com.continuuity.metrics2.collector;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.service.AbstractRegisteredServer;
import com.continuuity.common.service.RegisteredServerInfo;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.common.metrics.codec.MetricCodecFactory;
import org.apache.mina.core.filterchain.DefaultIoFilterChainBuilder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.integration.jmx.IoFilterMBean;
import org.apache.mina.integration.jmx.IoServiceMBean;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.*;

/**
 * An entry point for the server for collecting metrics.
 */
public final class MetricsCollectionServer extends AbstractRegisteredServer
    implements MetricsCollectionServerInterface {

  private static final Logger
      Log = LoggerFactory.getLogger(MetricsCollectionServer.class);

  /**
   * Defines thread pool size.
   */
  private static final int THREAD_POOL_SIZE = 250;

  /**
   * Defines idle thread pool size.
   */
  private static final int IDLE_THREAD_POOL_SIZE = 10;

  /**
   * Defines queue length
   */
  private static final int WORKER_QUEUE_LENGTH = 100000;

  /**
   * Latch determining whether server has shutdown or no.
   */
  private final CountDownLatch finished = new CountDownLatch(1);

  /**
   * Metric collection handler.
   */
  private MetricsCollectionServerIoHandler handler = null;

  /**
   * Socket acceptor.
   */
  private NioSocketAcceptor acceptor;

  @Override
  protected Thread start() {
    return new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          while(true) {
            if(finished.await(1, TimeUnit.SECONDS)) {
              break;
            }
          }
        } catch (InterruptedException e) {}
      }
    });
  }

  /**
   * Stops the metric collection server.
   */
  public void stop() {
    // Unbind from the port so that we don't receive any more
    // requests. Assumption is that the <code>AbstractRegisteredServer</code>
    // has already unregistered, so it's safe to unbind the port.
    if(acceptor != null) {
      acceptor.unbind();
      // latch down the counter.
      finished.countDown();
      // dispose of the acceptor
      acceptor.dispose();
    }
  }

  /**
   * Determines whether the server is still active, if not, stop() is invoked.
   *
   * @return true if running; else false.
   */
  @Override
  protected boolean ruok() {
    return acceptor.isActive();
  }

  /**
   * Configures the metrics collection server and registers the address and
   * port the server is running on.
   *
   * @param args command line arguments
   * @param conf configuration object.
   * @return instance of server info to be used for service registration.
   */
  @Override
  protected RegisteredServerInfo configure(String[] args, CConfiguration conf) {
    try {

      // Verify that an IoHandler has been set.
      if(handler == null) {
        // Create an instance of metrics i/o handler.
        handler =
          new MetricsCollectionServerIoHandler(conf);
      }

      // Retrieve the port on which to run the overlord server.
      int serverPort = conf.getInt( Constants.CFG_METRICS_COLLECTOR_SERVER_PORT,
                                    Constants.DEFAULT_METRICS_COLLECTOR_SERVER_PORT);

      InetAddress serverAddress = getServerInetAddress(
            conf.get(Constants.CFG_METRICS_COLLECTOR_SERVER_ADDRESS,
                     Constants.DEFAULT_METRICS_COLLECTOR_SERVER_ADDRESS)
          );

      // Create a JMX MBean Server instance.
      MBeanServer mBeanServer = handler.getmBeanServer();

      // Socket acceptor for handling incoming connections.
      acceptor = new NioSocketAcceptor();

      // Set to reuse address
      acceptor.setReuseAddress(true);

      // Configures to keep the connection alive if client wants to do
      // so.
      acceptor.getSessionConfig().setKeepAlive(true);

      // Disable packet batching.
      acceptor.getSessionConfig().setTcpNoDelay(true);

      // Create a JMX aware bean that wraps IoService object. In our case
      // it's NioSocketAcceptor.
      IoServiceMBean acceptorMBean = new IoServiceMBean(acceptor);

      if(mBeanServer != null) {
        // create a JMX ObjectName.  This has to be in a specific format.
        ObjectName acceptorName = new ObjectName(
          acceptor.getClass().getPackage().getName() +
            ":type=acceptor,name=" + acceptor.getClass().getSimpleName());

        // register the bean on the MBeanServer.
        mBeanServer.registerMBean(acceptorMBean, acceptorName);
      }

      // add an IoFilter .  This class is responsible for converting the incoming and
      // outgoing raw data to MetricRequest and MetricResponse objects
      ProtocolCodecFilter protocolFilter
        = new ProtocolCodecFilter(new MetricCodecFactory(false));

      // create a JMX-aware bean that wraps a MINA IoFilter object.  In this
      // case, a ProtocolCodecFilter
      IoFilterMBean protocolFilterMBean = new IoFilterMBean(protocolFilter);

      if(mBeanServer != null) {
        // create a JMX ObjectName.
        ObjectName protocolFilterName = new ObjectName(
          protocolFilter.getClass().getPackage().getName() +
            ":type=protocolfilter,name=" + protocolFilter.getClass().getSimpleName());

        // register the bean on the MBeanServer.  Without this line, no JMX will happen for
        // this filter.
        mBeanServer.registerMBean( protocolFilterMBean, protocolFilterName );
      }

      // Add an IoFilter - This class is responsible for converting
      // the incoming and outgoing raw data to MetricRequest and MetricResponse
      // objects.
      acceptor.getFilterChain().addLast("protocol", protocolFilter);

      // Get a reference to the filter chain from the acceptor.
      DefaultIoFilterChainBuilder filterChainBuilder =
        acceptor.getFilterChain();

      // Add an ExecutorFilter to the filter chain. The prefered order is to
      // put the executor filter after any protocol filter due to the fact that
      // protocol codec are generally CPU-bound which is the same as I/O filters.
      filterChainBuilder.addLast("threadPool",
                                 new ExecutorFilter(new ThreadPoolExecutor(
                                   IDLE_THREAD_POOL_SIZE, THREAD_POOL_SIZE,
                                   5*60, TimeUnit.SECONDS,
                                   new ArrayBlockingQueue<Runnable>(WORKER_QUEUE_LENGTH)
                                 )));

      // Set this NioSocketAcceptor's handler to the MetricCollectionServerIoHandler
      acceptor.setHandler(handler);

      // Bind to the specified address. This kicks of the listening for
      // the incoming connections.
      acceptor.bind(new InetSocketAddress(serverAddress, serverPort));

      // Set service name. This is the name by which the clients can find
      // me.
      setServerName(Constants.SERVICE_METRICS_COLLECTION_SERVER);

      // Add a shutdown hook for smooth shutdown.
      Runtime.getRuntime().addShutdownHook(
        new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              Log.info("Shutting down gracefully.");
              handler.close();
            } catch (IOException e) {
              Log.error("Failing closing the handler. Reason : {}",
                        e.getMessage());
            }
          }
        })
      );

      // Include the service payload.
      RegisteredServerInfo registrationInfo =
        new RegisteredServerInfo(serverAddress.getHostAddress(), serverPort);

      return registrationInfo;
    } catch (Exception e) {
      Log.error("Failed configuring metrics collection server : {}",
                e.getMessage(), e);
    }
    return null;
  }
}
