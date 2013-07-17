package com.continuuity.app.metrics;

import com.continuuity.api.metrics.MetricsCollectionService;
import com.continuuity.api.metrics.MetricsScope;

/**
 * Metrics collector for Flowlet
 */
public class FlowletMetrics extends AbstractProgramMetrics {

  public FlowletMetrics(MetricsCollectionService collectionService,
                        String applicationId, String flowId, String flowletId) {
    // Not support runID for now.
    super(collectionService.getCollector(
      MetricsScope.USER, String.format("%s.f.%s.%s", applicationId, flowId, flowletId), "0"));
  }
}
