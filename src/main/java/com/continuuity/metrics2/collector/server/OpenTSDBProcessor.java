package com.continuuity.metrics2.collector.server;

import com.continuuity.common.builder.BuilderException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.metrics2.collector.MetricRequest;
import com.continuuity.metrics2.collector.MetricResponse;
import com.continuuity.metrics2.collector.MetricType;
import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.session.IoSession;
import org.mortbay.log.Log;

import java.io.IOException;

/**
 * Concrete implementation of {@link MetricsProcessor} for forwarding
 * metrics to opentsdb.
 */
final class OpenTSDBProcessor implements MetricsProcessor {

  private final boolean enabled;
  private final boolean forwardFlowMetricsToOpenTSDB;
  private final String openTSDBServer;
  private final int openTSDBPort;
  private final OpenTSDBClient client;

  public OpenTSDBProcessor(CConfiguration configuration) {
    this.enabled = true;
    this.forwardFlowMetricsToOpenTSDB = configuration.getBoolean(
      Constants.CFG_METRICS_COLLECTOR_FORWARD_FLOW_TO_OPENTSDB,
      Constants.DEFAULT_METRICS_COLLECTOR_FORWARD_FLOW_TO_OPENTSDB
    );
    this.openTSDBServer = configuration.get(
      Constants.CFG_OPENTSDB_SERVER_ADDRESS,
      Constants.DEFAULT_OPENTSDB_SERVER_ADDRESS
    );
    this.openTSDBPort = configuration.getInt(
      Constants.CFG_OPENTSDB_SERVER_PORT,
      Constants.DEFAULT_OPENTSDB_SERVER_PORT
    );
    this.client = new OpenTSDBClient(openTSDBServer, openTSDBPort);
  }

  @Override
  public void process(final IoSession session, final MetricRequest request)
      throws IOException {

    // If forwarding to opentdsb not enabled, then we silent return
    // true saying that we have successfully processed.
    if(! enabled) {
      return;
    }

    // This is forwarding even the flow metrics to opentsdb.
    MetricType type = request.getMetricType();
    String command = null;
    if(type == MetricType.FlowSystem || type == MetricType.FlowUser) {
      // If forwarding not enabled, then the metric would be ignore.
      if(! forwardFlowMetricsToOpenTSDB)  {
        session.write(new MetricResponse(MetricResponse.Status.IGNORED));
      }
      try {
        // Constructs tag string that came on original request.
        String tags = "";
        for(ImmutablePair<String, String> tag : request.getTags()) {
          tags = tags + " " + tag.getFirst() + "=" + tag.getSecond();
        }

        // Breaks the metric into it's constituents.
        FlowMetricElements e =
          new FlowMetricElements.Builder(request.getMetricName()).create();
        command = String.format(
          "put %s %s %s acct=%s app=%s flow=%s flowlet=%s rid=%s iid=%d %s",
          e.getMetric(), request.getTimestamp(), request.getValue(),
          e.getAccountId(), e.getApplicationId(), e.getFlowId(), e.getFlowletId(),
          e.getRunId(), e.getInstanceId(), tags
          );
        session.write(new MetricResponse(MetricResponse.Status.SUCCESS));
      } catch (BuilderException e) {
        session.write(new MetricResponse(MetricResponse.Status.INVALID));
        return;
      }
    } else {
      command = request.getRawRequest();
    }

    // Send the metric to open tsdb and attache a write future
    // that will respond with to the callee.
    client.send(command).addListener(
      new IoFutureListener<WriteFuture>() {
        @Override
        public void operationComplete(WriteFuture future) {
          if(! future.isWritten()) {
            Log.warn("Failed to write metrics to opentsdb");
            session.write(new MetricResponse(MetricResponse.Status.FAILED));
          } else {
            session.write(new MetricResponse(MetricResponse.Status.SUCCESS));
          }
          future.setWritten();
        }
      }
    );
  }

}
