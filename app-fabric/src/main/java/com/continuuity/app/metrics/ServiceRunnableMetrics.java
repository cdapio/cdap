package com.continuuity.app.metrics;

import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsScope;

/**
 * Metrics Collector for Service.
 */
public class ServiceRunnableMetrics extends AbstractProgramMetrics {

  public ServiceRunnableMetrics(MetricsCollectionService collectionService, String applicationId,
                                String serviceId, String runnableId) {
    super(collectionService.getCollector(
      MetricsScope.USER, String.format("%s.s.%s.%s", applicationId, serviceId, runnableId), "0"));
  }
}
