package com.continuuity.metrics2.collector.client;

import com.continuuity.metrics2.collector.MetricResponse;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link IoHandler} implementation of Metrics client. This class
 * is extended from {@link org.apache.mina.core.service.IoHandlerAdapter}
 * for convenience.
 */
public class MetricsProtocolHandler extends IoHandlerAdapter {
  private static final Logger Log =
    LoggerFactory.getLogger(MetricsProtocolHandler.class);

  @Override
  public void exceptionCaught(IoSession session, Throwable cause) throws
    Exception {
    Log.warn(cause.getMessage(), cause);
  }

  @Override
  public void messageReceived(IoSession session, Object message) {
    if(message instanceof MetricResponse) {
      MetricResponse response = (MetricResponse) message;
      if(response.getStatus() == MetricResponse.Status.FAILED) {
        Log.warn("Failed processing metric on the overlord server. Look at "
                   + "server logs.");
      } else if(response.getStatus() == MetricResponse.Status.IGNORED) {
        Log.warn("Metrics collection server ignore the metric type as there " +
                   "was no appropriate processor attached for processing.");
      }
    }
  }

}
