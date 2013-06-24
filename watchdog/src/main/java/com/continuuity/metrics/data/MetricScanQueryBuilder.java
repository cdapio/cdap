package com.continuuity.metrics.data;

import com.google.common.base.Preconditions;

/**
 * A builder for creating instance of {@link MetricScanQuery}.
 */
public final class MetricScanQueryBuilder {

  private String contextPrefix;
  private String metricPrefix;
  private String tagPrefix;

  public MetricScanQueryBuilder setContext(String context) {
    this.contextPrefix = context;
    return this;
  }

  public MetricScanQueryBuilder setMetric(String metric) {
    this.metricPrefix = metric;
    return this;
  }

  public MetricScanQueryBuilder setTag(String tag) {
    this.tagPrefix = tag;
    return this;
  }

  public MetricScanQuery build(final long startTime, final long endTime) {
    Preconditions.checkArgument(startTime <= endTime, "Invalid time range.");
    Preconditions.checkState(contextPrefix != null, "Context prefix not set.");
    Preconditions.checkState(metricPrefix != null, "Tag prefix not set.");

    final String finalContextPrefix = contextPrefix;
    final String finalMetricPrefix = metricPrefix;
    final String finalTagPrefix = tagPrefix;

    return new MetricScanQuery() {
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
