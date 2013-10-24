package com.continuuity.app.metrics;

import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsScope;

/**
 * Metrics collector for Flowlet.
 */
public class ProcedureMetrics extends AbstractProgramMetrics {

  public ProcedureMetrics(MetricsCollectionService collectionService, String applicationId, String procedureId) {
    // Not support runID for now.
    super(collectionService.getCollector(MetricsScope.USER, String.format("%s.p.%s", applicationId, procedureId), "0"));
  }
}
