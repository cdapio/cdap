package com.continuuity.app.metrics;

import com.continuuity.common.metrics.AbstractCMetrcisBasedMetrics;

/**
 * Metrics collector for Flowlet
 */
public class FlowletMetrics extends AbstractCMetrcisBasedMetrics {

  public FlowletMetrics(final String accountId,
                           final String applicationId,
                           final String flowId,
                           final String flowletId,
                           final String pid,
                           final int instanceId) {
    super(String.format("%s.%s.%s.%s.%s.%d",
                        accountId,
                        applicationId,
                        flowId,
                        pid,
                        flowletId,
                        instanceId));
  }
}
