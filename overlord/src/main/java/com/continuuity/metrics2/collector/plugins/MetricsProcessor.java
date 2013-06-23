package com.continuuity.metrics2.collector.plugins;

import akka.dispatch.Future;
import com.continuuity.common.metrics.MetricRequest;
import com.continuuity.common.metrics.MetricResponse;
import com.google.common.util.concurrent.ListenableFuture;

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
  public ListenableFuture<MetricResponse.Status> process(final MetricRequest request)
  throws IOException;
}
