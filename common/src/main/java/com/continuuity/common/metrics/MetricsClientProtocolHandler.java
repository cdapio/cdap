package com.continuuity.common.metrics;

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
public class MetricsClientProtocolHandler extends IoHandlerAdapter {
  private static final Logger Log =
    LoggerFactory.getLogger(MetricsClientProtocolHandler.class);

  /**
   * Callback invoked when there is issue in processing the messages.
   *
   * @param session handle associated with current communication.
   * @param cause A throwable describing the issue.
   * @throws Exception
   */
  @Override
  public void exceptionCaught(IoSession session, Throwable cause) throws
    Exception {
    Log.warn(cause.getMessage(), cause);
  }

  /**
   * Message handler that is passed the application level object by the code.
   *
   * @param session handle associated with current communication.
   * @param message object
   */
  @Override
  public void messageReceived(IoSession session, Object message) {
    if (message instanceof MetricResponse) {
      MetricResponse response = (MetricResponse) message;
      if (response.getStatus() == MetricResponse.Status.FAILED) {
        Log.warn("Failed processing metric on the overlord server. Request "
                   + "server logs.");
      } else if (response.getStatus() == MetricResponse.Status.IGNORED) {
        Log.warn("Server ignored the data point due to capacity.");
      } else if (response.getStatus() == MetricResponse.Status.INVALID) {
        Log.warn("Invalid request was sent to the server.");
      } else if (response.getStatus() == MetricResponse.Status.SERVER_ERROR) {
        Log.warn("Internal server error.");
      }
    } else {
      Log.warn("Invalid message received from the server. Message : {}",
               message);
    }
  }

}
