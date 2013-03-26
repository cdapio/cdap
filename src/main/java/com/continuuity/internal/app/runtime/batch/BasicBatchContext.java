package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.app.logging.MapReduceLoggingContext;
import com.continuuity.app.metrics.MapReduceMetrics;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.RunId;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 *
 */
public class BasicBatchContext {
  private final String accountId;
  private final String applicationId;
  private final String programName;
  private final RunId runId;
  private final Map<String, DataSet> datasets;
  private final CMetrics systemMetrics;
  private final MapReduceMetrics metrics;
  private final MapReduceLoggingContext loggingContext;

  public BasicBatchContext(Program program, RunId runId, Map<String, DataSet> datasets) {
    this.accountId = program.getAccountId();
    this.applicationId = program.getApplicationId();
    this.programName = program.getProgramName();
    this.runId = runId;
    this.datasets = ImmutableMap.copyOf(datasets);
    // FIXME
    this.systemMetrics = new CMetrics(MetricType.FlowSystem, getMetricName());
    this.metrics = new MapReduceMetrics(getAccountId(), getApplicationId(),
                                           getProgramName(), getRunId().toString(), getInstanceId());
    this.loggingContext = new MapReduceLoggingContext(getAccountId(), getApplicationId(), getProgramName());
  }

  @Override
  public String toString() {
    return String.format("accountId=%s, applicationId=%s, program=%s, runid=%s",
                         accountId, applicationId, programName, runId);
  }

  public <T extends DataSet> T getDataSet(String name) {
    T dataSet = (T) datasets.get(name);
    Preconditions.checkArgument(dataSet != null, "%s is not a known DataSet.", name);
    return dataSet;

  }

  public Metrics getMetrics() {
    return metrics;
  }

  public CMetrics getSystemMetrics() {
    return systemMetrics;
  }

  public String getAccountId() {
    return accountId;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getProgramName() {
    return programName;
  }

  public RunId getRunId() {
    return runId;
  }

  public int getInstanceId() {
    return 0;
  }

  public LoggingContext getLoggingContext() {
    return loggingContext;
  }

  private String getMetricName() {
    // FIXME
    return String.format("%s.%s.%s.%s.foo.%d",
                         getAccountId(),
                         getApplicationId(),
                         getProgramName(),
                         getRunId(),
                         getInstanceId());
  }
}
