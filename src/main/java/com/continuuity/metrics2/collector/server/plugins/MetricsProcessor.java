package com.continuuity.metrics2.collector.server.plugins;

import akka.dispatch.Future;
import com.continuuity.metrics2.collector.MetricRequest;
import com.continuuity.metrics2.collector.MetricResponse;
import org.apache.mina.core.session.IoSession;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface defining processing of a metric.
 */
public interface MetricsProcessor extends Closeable {

  /**
   * Processes a {@link MetricRequest}.
   *
   * <p>
   *   Processing a metric returns a Future (an object holding the future of
   *   processing).
   * </p>
   *
   * @param request that needs to be processed.
   * @return Future for asynchronous processing of metric.
   * @throws IOException
   */
  public Future<MetricResponse.Status> process(final MetricRequest request)
  throws IOException;
}
