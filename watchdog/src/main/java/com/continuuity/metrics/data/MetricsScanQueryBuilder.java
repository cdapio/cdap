package com.continuuity.metrics.data;

import com.google.common.base.Preconditions;

/**
 * A builder for creating instance of {@link MetricsScanQuery}.
 */
public final class MetricsScanQueryBuilder {

  private String contextPrefix;
  private String runId;
  private String metricPrefix;
  private String tagPrefix;
  private boolean allowEmptyMetric = false;

  public MetricsScanQueryBuilder setContext(String context) {
    this.contextPrefix = context;
    return this;
  }

  public MetricsScanQueryBuilder setRunId(String runId) {
    this.runId = runId;
    return this;
  }

  public MetricsScanQueryBuilder setMetric(String metric) {
    this.metricPrefix = metric;
    return this;
  }

  public MetricsScanQueryBuilder setTag(String tag) {
    this.tagPrefix = tag;
    return this;
  }

  public MetricsScanQueryBuilder allowEmptyMetric() {
    this.allowEmptyMetric = true;
    return this;
  }

  public MetricsScanQuery build(final long startTime, final long endTime) {
    Preconditions.checkArgument(startTime <= endTime, "Invalid time range.");
    Preconditions.checkState(allowEmptyMetric || metricPrefix != null, "Metrics prefix not set.");

    final String finalContextPrefix = contextPrefix;
    final String finalRunId = runId;
    final String finalMetricPrefix = metricPrefix;
    final String finalTagPrefix = tagPrefix;

    return new MetricsScanQuery() {
      @Override
      public long getStartTime() {
        return startTime;
      }

      @Override
      public long getEndTime() {
        return endTime;
      }

      @Override
      public String getContextPrefix() {
        return finalContextPrefix;
      }

      @Override
      public String getRunId() {
        return finalRunId;
      }

      @Override
      public String getMetricPrefix() {
        return finalMetricPrefix;
      }

      @Override
      public String getTagPrefix() {
        return finalTagPrefix;
      }
    };
  }
}
