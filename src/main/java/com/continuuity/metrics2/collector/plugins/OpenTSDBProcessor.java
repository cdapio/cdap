package com.continuuity.metrics2.collector.plugins;

import akka.dispatch.ExecutionContext;
import akka.dispatch.ExecutionContexts;
import akka.dispatch.Future;
import akka.dispatch.Futures;
import com.continuuity.common.builder.BuilderException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.common.metrics.MetricRequest;
import com.continuuity.common.metrics.MetricResponse;
import com.continuuity.common.metrics.MetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Concrete implementation of
 * {@link com.continuuity.metrics2.collector.plugins.MetricsProcessor}
 * for forwarding metrics to opentsdb.
 */
public final class OpenTSDBProcessor implements MetricsProcessor {
  private static final Logger Log = LoggerFactory.getLogger(
    OpenTSDBProcessor.class
  );
  private final String openTSDBServer;
  private final int openTSDBPort;
  private final OpenTSDBClient client;

  /**
   * Executor service instance.
   */
  private final ExecutorService es = Executors.newFixedThreadPool(50);

  /**
   * Execution context under which the DB updates will happen.
   */
  private final ExecutionContext ec
    = ExecutionContexts.fromExecutorService(es);

  public OpenTSDBProcessor(CConfiguration configuration) {
    // Retrieve server and port from confiuguration.
    this.openTSDBServer = configuration.get(
      Constants.CFG_OPENTSDB_SERVER_ADDRESS,
      Constants.DEFAULT_OPENTSDB_SERVER_ADDRESS
    );

    this.openTSDBPort = configuration.getInt(
      Constants.CFG_OPENTSDB_SERVER_PORT,
      Constants.DEFAULT_OPENTSDB_SERVER_PORT
    );

    // Creates an openTSDB client.
    this.client = new OpenTSDBClient(openTSDBServer, openTSDBPort);
  }

  @Override
  public Future<MetricResponse.Status> process(final MetricRequest request)
      throws IOException {

    // Future that returns an invalid status.
    final Future<MetricResponse.Status> invalidFutureResponse =
      Futures.future(new Callable<MetricResponse.Status>() {
        @Override
        public MetricResponse.Status call() throws Exception {
          return MetricResponse.Status.INVALID;
        }
      }, ec);

    // This is forwarding even the flow metrics to opentsdb.
    MetricType type = request.getMetricType();
    final String command;
    if(type == MetricType.FlowSystem || type == MetricType.FlowUser) {
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
      } catch (BuilderException e) {
        return Futures.future(new Callable<MetricResponse.Status>() {
          @Override
          public MetricResponse.Status call() throws Exception {
            return MetricResponse.Status.INVALID;
          }
        }, null);
      }
    } else if(type == MetricType.System) {
      // When it's a system, we directly report that to opentsdb.
      command = request.getRawRequest();
    } else {
      // Will force to return a invalidFutureResponse.
      command = null;
    }

    // We execute the command in a thread an return the future the
    // MetricCollectionServerIoHandler which attaches a completion
    // handler.
    if(command != null) {
      return Futures.future(new Callable<MetricResponse.Status>() {
        @Override
        public MetricResponse.Status call() throws Exception {
          try {
            client.send(command).await();
          } catch (InterruptedException e) {
            return MetricResponse.Status.FAILED;
          } catch (IOException e) {
            return MetricResponse.Status.FAILED;
          }
          return MetricResponse.Status.SUCCESS;
        }
      }, ec);
    }
    return invalidFutureResponse;
  }

  @Override
  public void close() throws IOException {
    if(es != null) {
      es.shutdown();
    }
  }
}
