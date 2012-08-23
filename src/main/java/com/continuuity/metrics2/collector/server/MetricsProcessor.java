package com.continuuity.metrics2.collector.server;

import com.continuuity.metrics2.collector.MetricRequest;
import org.apache.mina.core.session.IoSession;

import java.io.IOException;

/**
 * Interface defining processing of a metric.
 */
interface MetricsProcessor {
  /**
   * Processes the metric.
   *
   * @param request metric request that needs to be processed.
   */
  public void process(final IoSession session, final MetricRequest request)
        throws IOException;
}
