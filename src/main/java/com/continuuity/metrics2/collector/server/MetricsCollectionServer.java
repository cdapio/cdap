package com.continuuity.metrics2.collector.server;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.service.AbstractRegisteredServer;
import com.continuuity.common.service.RegisteredServerInfo;
import com.continuuity.metrics2.collector.codec.MetricCodecFactory;
import org.apache.mina.core.filterchain.DefaultIoFilterChainBuilder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.integration.jmx.IoFilterMBean;
import org.apache.mina.integration.jmx.IoServiceMBean;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * An entry point for the server for collecting metrics.
 */
public final class MetricsCollectionServer extends AbstractRegisteredServer {
  private static final Logger
      Log = LoggerFactory.getLogger(MetricsCollectionServer.class);

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
    acceptor.unbind();

    // latch down the counter.
    finished.countDown();

    // dispose of the acceptor
    acceptor.dispose();
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
   * Sets the metric Io handler to be used for receiving and sending data.
   *
   * @param handler instance of handler.
   */
  public void setIoHandler(MetricsCollectionServerIoHandler handler) {
    this.handler = handler;
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
        throw new Exception("No IoHandler has been set. " +
                              "Set using #setIoHandler API");
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
                                 new ExecutorFilter(Executors
                                                      .newCachedThreadPool()));

      // Set this NioSocketAcceptor's handler to the MetricCollectionServerIoHandler
      acceptor.setHandler(handler);

      // Bind to the specified address. This kicks of the listening for
      // the incoming connections.
      acceptor.bind(new InetSocketAddress(serverAddress, serverPort));

      // Set service name. This is the name by which the clients can find
      // me.
      setServerName("metriccollector");

      // Include the service payload.
      RegisteredServerInfo registrationInfo =
        new RegisteredServerInfo(serverAddress.getHostAddress(), serverPort);

      return registrationInfo;
    } catch (Exception e) {
      Log.error(e.getMessage());
    }
    return null;
  }
}
