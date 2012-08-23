package com.continuuity.metrics2.collector.server;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.metrics2.collector.MetricRequest;
import com.continuuity.metrics2.collector.MetricResponse;
import com.continuuity.metrics2.collector.MetricType;
import com.google.common.collect.Maps;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.integration.jmx.IoSessionMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.util.Map;

/**
 * Handler for metrics collection server.
 */
public final class MetricsCollectionServerIoHandler extends IoHandlerAdapter {
  private static final Logger Log
    = LoggerFactory.getLogger(MetricsCollectionServerIoHandler.class);

  /**
   * Holds the instance of MBeanServer
   */
  private MBeanServer mBeanServer;

  /**
   * Instance of {@link CConfiguration} object.
   */
  private final CConfiguration configuration;

  /**
   * List of processor mapping to the type of metric they can process.
   */
  private final Map<MetricType, MetricsProcessor> processors
            = Maps.newHashMap();

  /**
   * Creates a new instance of {@code MetricCollectionServerIoHandler}.
   * <p>
   *   We pass in a reference to the MBeanServer. This instance will be
   *   used to register new IoSession objects so that the JMX subsystem
   *   can report statistics on the sessions.
   * </p>
   * @param mBeanServer
   * @param configuration
   */
  public MetricsCollectionServerIoHandler(MBeanServer mBeanServer,
                                          CConfiguration configuration)
    throws Exception {

    this.mBeanServer = mBeanServer;
    this.configuration = configuration;

    // Add the mapping from metric type to their respective processors.
    MetricsProcessor openTSDB = new OpenTSDBProcessor(configuration);

    // If configured to send flow metrics to opentsdb then we set the
    // right procesor for it.
    if(configuration.getBoolean(
      Constants.CFG_METRICS_COLLECTOR_FORWARD_FLOW_TO_OPENTSDB,
      Constants.DEFAULT_METRICS_COLLECTOR_FORWARD_FLOW_TO_OPENTSDB
    )) {
      Log.info("Configuring to send flow metrics to opentsdb.");
      add(MetricType.FlowSystem, openTSDB);
      add(MetricType.FlowSystem, openTSDB);
    } else {
      MetricsProcessor flow = new FlowMetricsProcessor(configuration);
      add(MetricType.FlowSystem, flow);
      add(MetricType.FlowUser, flow);
    }
    add(MetricType.System, openTSDB);
  }

  /**
   * Creates a new instance of this object without bean tracking.
   *
   * @param configuration
   */
  public MetricsCollectionServerIoHandler(CConfiguration configuration)
    throws Exception {
    this(null, configuration);
  }

  /**
   * @return Instance of MBean server created during initialization.
   */
  public MBeanServer getmBeanServer() {
    return mBeanServer;
  }

  /**
   * Adds a metric type and metric processor mapping.
   *
   * @param type of the metric
   * @param processor associated with the metric.
   */
  private void add(MetricType type, MetricsProcessor processor) {
    processors.put(type, processor);
  }

  /**
   * This method is called first when a new connection to the server is made.
   * <p>
   *   We set the the JMX session MBean.
   * </p>
   * @param session
   * @throws Exception
   */
  @Override
  public void sessionCreated(IoSession session) throws Exception {
    // If no bean server created then no tracking is done.
    if(mBeanServer == null) {
      return;
    }

    // Create a session MBean in order to load into the MBeanServer and
    // allow this session to be managed by the JMX subsystem.
    IoSessionMBean sessionMBean = new IoSessionMBean(session);

    // Create a JMX ObjectName. This has to be in specific format.
    ObjectName sessionName = new ObjectName(
      session.getClass().getPackage().getName() + ":type=session, name=" +
        session.getClass().getSimpleName() + "-" + session.getId()
    );

    // register the bean on the MBeanServer.
    mBeanServer.registerMBean(sessionMBean, sessionName);
  }

  @Override
  public void exceptionCaught(IoSession session, Throwable cause) throws
    Exception {
    Log.warn(cause.getMessage(), cause);
  }

  /**
   * Processes the message received by the collection server.
   *
   * @param session
   * @param message
   * @throws Exception
   */
  @Override
  public void messageReceived(IoSession session, Object message) throws
    Exception {

    if(message instanceof MetricRequest) {
      MetricRequest request = (MetricRequest) message;
      // If the request is valid.
      if(request.getValid()) {
        // We see if there is a valid processor associated with the
        // metric type. If there is any, then we use that to process
        // the metric, else we return a flag indicating that the
        // metric has been ignored.
        if(processors.containsKey(request.getMetricType())) {
          processors.get(request.getMetricType()).process(
            session, request
          );
        } else {
          session.write(new MetricResponse(MetricResponse.Status.IGNORED));
        }
      }
    }
  }

}
