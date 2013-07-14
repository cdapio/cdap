package com.continuuity.metrics.data;

/**
 * Represents a scanning query to the {@link MetricsTable}.
 */
public interface MetricsScanQuery {

  long getStartTime();

  long getEndTime();

  String getContextPrefix();

  String getRunId();

  String getMetricPrefix();

  String getTagPrefix();
}
