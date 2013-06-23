package com.continuuity.app.metrics;

import com.continuuity.common.metrics.AbstractCMetrcisBasedMetrics;

/**
 * Metrics collector for MapReduce job
 */
public class MapReduceMetrics extends AbstractCMetrcisBasedMetrics {

  public MapReduceMetrics(final String accountId, final String applicationId, final String mapReduceId,
                          final String pid, final int instanceId) {
    super(String.format("%s.%s.%s.%s.%d",
                        accountId,
                        applicationId,
                        mapReduceId,
                        pid,
                        instanceId));
  }
}
