package com.continuuity.metrics2.collector;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricRequest;
import com.continuuity.common.metrics.MetricResponse;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.metrics2.collector.plugins.MetricsProcessor;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.integration.jmx.IoSessionMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Handler for metrics collection server.
 */
public final class MetricsCollectionServerIoHandler extends IoHandlerAdapter
  implements Closeable {
  private static final Logger Log
    = LoggerFactory.getLogger(MetricsCollectionServerIoHandler.class);

  /**
   * Holds the instance of MBeanServer.
   */
  private MBeanServer mBeanServer;

  /**
   * Instance of {@link CConfiguration} object.
   */
  private final CConfiguration configuration;

  /**
   * List of processor mapping to the type of metric they can process.
   */
  private final List<ImmutablePair<MetricType, MetricsProcessor>>
      processors = Lists.newCopyOnWriteArrayList();

  /**
   * Creates a new instance of {@code MetricCollectionServerIoHandler}.
   * <p>
   *   We pass in a reference to the MBeanServer. This instance will be
   *   used to register new IoSession objects so that the JMX subsystem
   *   can report statistics on the sessions.
   * </p>
   */
  public MetricsCollectionServerIoHandler(MBeanServer mBeanServer,
                                          CConfiguration configuration)
    throws Exception {

    this.mBeanServer = mBeanServer;
    this.configuration = configuration;

    // Read in the processors to be used for managing system metrics.
    // If there are none defined, then we don't assign defaults. If
    // there any defined, then we create instance of processor and
    // pass in the configuration object to it.
    String[] klassSystem = configuration.getStrings(
        Constants.CFG_METRICS_COLLECTION_SYSTEM_PLUGINS
    );
    if (klassSystem != null && klassSystem.length > 0) {
      for (String klass : klassSystem) {
        loadCreateAndAddToList(MetricType.System, klass);
        Log.trace("Added {} plugin for processing system metrics.",
                  klass);
      }
    }

    // Load processor for handling flow system metrics. If none defined,
    // we add a default processor.
    String[] klassFlowSystem = configuration.getStrings(
      Constants.CFG_METRICS_COLLECTION_FLOW_SYSTEM_PLUGINS
    );
    if (klassFlowSystem != null && klassFlowSystem.length > 0) {
      for (String klass : klassFlowSystem) {
        loadCreateAndAddToList(MetricType.FlowSystem, klass);
        Log.trace("Added {} plugin for processing flow system metrics.",
                  klass);
      }
    }

    // Load processor for handling flow user metrics. If none defined,
    // we add a default processor.
    String[] klassFlowUser = configuration.getStrings(
      Constants.CFG_METRICS_COLLECTION_FLOW_USER_PLUGINS
    );

    if (klassFlowUser != null && klassFlowUser.length > 0) {
      for (String klass : klassFlowUser) {
        loadCreateAndAddToList(MetricType.FlowUser, klass);
        Log.trace("Added {} plugin for processing flow user metrics.",
                  klass);
      }
    }
  }

  private void loadCreateAndAddToList(MetricType type, String klassName)
    throws Exception {
    MetricsProcessor processor
      = (MetricsProcessor) Class.forName(klassName)
      .getDeclaredConstructor(CConfiguration.class)
      .newInstance(configuration);
    add(type, processor);
  }

  /**
   * Creates a new instance of this object without bean tracking.
   *
   * @param configuration object.
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
    processors.add(new ImmutablePair<MetricType, MetricsProcessor>(type,
                   processor));
  }

  /**
   * This method is called first when a new connection to the server is made.
   * <p>
   *   We set the the JMX session MBean.
   * </p>
   * @param session current session for which the callback was made.
   * @throws Exception
   */
  @Override
  public void sessionCreated(IoSession session) throws Exception {
    // If no bean server created then no tracking is done.
    if (mBeanServer == null) {
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
    if (cause != null && cause.getMessage() != null) {
      Log.warn(cause.getMessage(), cause);
    }
  }

  /**
   * Processes the message received by the collection server.
   *
   * @param session Session for which the message was received.
   * @param message Object for the session.
   * @throws Exception
   */
  @Override
  public void messageReceived(final IoSession session, final Object message)
    throws Exception {

    if (message instanceof MetricRequest) {
      final MetricRequest request = (MetricRequest) message;
      Log.trace("Received metric : {}.", request.toString());

      // If we have a valid request then we iterate through all the
      // processor attached to the metric type to process the metric.
      // Each processor will return a future that is chained to form
      // one response back to the client. There is no transactionality
      // in terms of processing the request. If there is any issue in
      // one of the future in processing the request, we return failure
      // to the client.
      if (request.getValid()) {
        ListenableFuture<MetricResponse.Status> future;

        // Iterate through the processor invoking the process method
        for (final ImmutablePair<MetricType, MetricsProcessor> processor :
          processors) {

          // If request metric type matches, then farm out the work
          // to the processor. If the
          if (request.getMetricType() == processor.getFirst()) {
            future = processor.getSecond().process(request);
            Futures.addCallback(future, new FutureCallback<MetricResponse.Status>() {
              @Override
              public void onSuccess(MetricResponse.Status result) {
                writeIfConnected(session, new MetricResponse(result));
              }

              @Override
              public void onFailure(Throwable t) {
                writeIfConnected(session,  new MetricResponse(MetricResponse.Status.FAILED));
              }
            }, MoreExecutors.sameThreadExecutor());

          }
        }
      } else {
        // if we are here that means either the request was invalid or the type
        // of request is not MetricRequest type. In this case we return an
        // status as INVALID to caller.
        writeIfConnected(
          session,
          new MetricResponse(MetricResponse.Status.INVALID)
        );
      }
    }
  }

  private void writeIfConnected(IoSession session, MetricResponse response) {
    if (session.isConnected()) {
      session.write(response);
    }
  }

  @Override
  public void close() throws IOException {
    for (ImmutablePair<MetricType, MetricsProcessor> processor : processors) {
      processor.getSecond().close();
    }
  }
}
