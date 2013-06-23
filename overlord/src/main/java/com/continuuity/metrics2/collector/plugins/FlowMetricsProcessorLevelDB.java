package com.continuuity.metrics2.collector.plugins;

import akka.dispatch.ExecutionContext;
import akka.dispatch.ExecutionContexts;
import akka.dispatch.Future;
import akka.dispatch.Futures;
import com.continuuity.common.builder.BuilderException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricRequest;
import com.continuuity.common.metrics.MetricResponse;
import com.continuuity.metrics2.temporaldb.DataPoint;
import com.continuuity.metrics2.temporaldb.TemporalDataStore;
import com.continuuity.metrics2.temporaldb.internal.LevelDBTemporalDataStore;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Adds flow metrics process to levelDB temporal store.
 */
public class FlowMetricsProcessorLevelDB implements MetricsProcessor {
  private static final Logger Log
    = LoggerFactory.getLogger(FlowMetricsProcessorLevelDB.class);

  /**
   * Instance of temporal database.
   */
  private static TemporalDataStore DB;

  /**
   * Executor service instance.
   */
  private final ExecutorService es = Executors.newFixedThreadPool(50);

  /**
   * Execution context under which the DB updates will happen.
   */
  private final ExecutionContext ec
    = ExecutionContexts.fromExecutorService(es);

  /**
   * Allowed time series metrics.
   */
  private Map<String, Boolean> allowedTimeseriesMetrics = Maps.newHashMap();

  /**
   * Constructs and initializes a flow metric processor.
   *
   * @param configuration objects
   * @throws Exception in case of sql errors.
   */
  public FlowMetricsProcessorLevelDB(CConfiguration configuration)
    throws Exception {

    String levelDBDir = configuration.get(
      "overlord.temporal.store.leveldb.path",
      "data"
    );

    // Open the levelDB temporal store. It will be created if
    // not exists.
    DB = new LevelDBTemporalDataStore(new File(levelDBDir));
    DB.open(CConfiguration.create());

    String[] allowedMetrics
      = configuration.getStrings(
      Constants.CFG_METRICS_COLLECTION_ALLOWED_TIMESERIES_METRICS,
      Constants.DEFAULT_METRICS_COLLECTION_ALLOWED_TIMESERIES_METRICS
    );

    for(String metric : allowedMetrics) {
      allowedTimeseriesMetrics.put(metric, true);
    }
  }

  /**
   * Invoked when a {@link MetricRequest} type matches either the
   * <code>FlowSystem</code> or <code>FlowUser</code>.
   *
   * @param request that needs to be processed.
   * @return A future of writing the metric to DB with processing status.
   * @throws java.io.IOException
   */
  @Override
  public ListenableFuture<MetricResponse.Status> process(final MetricRequest request)
    throws IOException {
    // Future that returns an invalid status.
    SettableFuture<MetricResponse.Status> response = SettableFuture.create();

    // Break down the metric name into it's components.
    // If there are any issue with how it's constructed,
    // send a failure back and log a message on the server.
    Log.trace("Received flow metric {}", request.toString());
    try {
      final FlowMetricElements elements =
        new FlowMetricElements.Builder(request.getMetricName()).create();
      if(elements != null) {
        if(updateDataPoint(elements, request)) {
          response.set(MetricResponse.Status.SUCCESS);
        } else {
          response.set(MetricResponse.Status.FAILED);
        }
      } else {
        Log.trace("Invalid flow metric elements for request {}",
                  request.toString());
      }
    } catch (BuilderException e) {
      Log.warn("Invalid flow metric received. Reason : {}.", e.getMessage());
    }
    response.set(MetricResponse.Status.FAILED);
    return response;
  }

  private boolean updateDataPoint(FlowMetricElements elements,
                                  MetricRequest request) {
    // Add the current value on to a known future time.
    {
      DataPoint.Builder builder = new DataPoint.Builder(elements.getMetric());
      builder.addTimestamp(Long.MAX_VALUE);
      builder.addValue(request.getValue());
      builder.addTag("acct", elements.getAccountId());
      builder.addTag("app", elements.getApplicationId());
      builder.addTag("flow", elements.getFlowId());
      builder.addTag("runid", elements.getRunId());
      builder.addTag("flowlet", elements.getFlowletId());
      builder.addTag("instance", String.valueOf(elements.getInstanceId()));
      try {
        DB.put(builder.create());
      } catch (Exception e) {
        Log.warn("Failed adding datapoint to temporal store. Reason : {}",
                 e.getMessage());
        return false;
      }
    }

    // Add the current value to the temporal timeseries for this metric.
    {
      DataPoint.Builder builder = new DataPoint.Builder(elements.getMetric());
      builder.addTimestamp(Long.MAX_VALUE);
      builder.addValue(request.getValue());
      builder.addTag("acct", elements.getAccountId());
      builder.addTag("app", elements.getApplicationId());
      builder.addTag("flow", elements.getFlowId());
      builder.addTag("runid", elements.getRunId());
      builder.addTag("flowlet", elements.getFlowletId());
      builder.addTag("instance", String.valueOf(elements.getInstanceId()));
      try {
        DB.put(builder.create());
      } catch (Exception e) {
        Log.warn("Failed adding datapoint to temporal store. Reason : {}",
                 e.getMessage());
        return false;
      }
    }
    return true;
  }

  /**
   * Closes the DB
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if(DB != null) {
      DB.close();
    }
    if(es != null) {
      es.shutdown();
    }
  }
}
