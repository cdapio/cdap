package com.continuuity.metrics.data;

/**
 * Represents a scanning query to the {@link TimeSeriesTable}.
 */
public interface MetricsScanQuery {

  long getStartTime();

  long getEndTime();

  String getContextPrefix();

  String getRunId();

  String getMetricPrefix();

  String getTagPrefix();
}
