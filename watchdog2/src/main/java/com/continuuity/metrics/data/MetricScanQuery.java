package com.continuuity.metrics.data;

/**
 * Represents a scanning query to the {@link MetricTable}.
 */
public interface MetricScanQuery {

  long getStartTime();

  long getEndTime();

  String getContextPrefix();

  String getMetricPrefix();

  String getTagPrefix();
}
