package com.continuuity.metrics2.collector.server;

import com.continuuity.common.builder.BuilderException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.metrics2.collector.MetricRequest;
import com.continuuity.metrics2.collector.MetricResponse;
import org.apache.mina.core.session.IoSession;
import org.mortbay.log.Log;

import java.io.IOException;

/**
 * Concrete implementation of {@link MetricsProcessor} for processing
 * the metrics received by {@link MetricsCollectionServer} of type
 * {@code MetricType.FlowSystem} and {@code MetricType.FlowSystem}.
 *
 * <p>
 *   Metrics are essentially written to a backend database that supports
 *   SQL.
 * </p>
 */
final class FlowMetricsProcessor implements MetricsProcessor {
  private CConfiguration configuration;
  private final String jdbcUrl;

  public FlowMetricsProcessor(CConfiguration configuration)
    throws ClassNotFoundException {

    this.configuration = configuration;
    this.jdbcUrl = configuration.get("overlord.jdbc.url", null);

    // Dynamically load the appropriate driver based on
    // whether it's hyperdb sql or mysql.
    if(jdbcUrl != null) {
      if(jdbcUrl.contains("hsqldb")) {
        Class.forName("org.hsqldb.jdbcDriver");
      } else if(jdbcUrl.contains("mysql")) {
        Class.forName("com.mysql.jdbc.Driver");
      }
    }
  }

  @Override
  public void process(IoSession session, MetricRequest request)
      throws IOException {

    // Break down the metric name into it's components.
    // If there are any issue with how it's constructed,
    // send a failure back and log a message on the server.
    try {
      FlowMetricElements elements =
          new FlowMetricElements.Builder(request.getMetricName()).create();
      Log.info("Successfully processed metric {}", elements.toString());
      session.write(new MetricResponse(MetricResponse.Status.SUCCESS));
    } catch (BuilderException e) {
      sendFailure(e, session);
      return;
    }

  }

  private void sendFailure(Throwable t, IoSession session) {
    sendFailure(t.getMessage(), session);
  }

  private void sendFailure(String message, IoSession session) {
    Log.warn(message);
    session.write(new MetricResponse(MetricResponse.Status.FAILED));
  }

  private void sendSuccess(IoSession session) {
    session.write(new MetricResponse(MetricResponse.Status.SUCCESS));
  }
}
