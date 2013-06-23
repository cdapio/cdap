package com.continuuity.app.metrics;

import com.continuuity.common.metrics.AbstractCMetrcisBasedMetrics;

/**
 * Metrics collector for Flowlet
 */
public class ProcedureMetrics extends AbstractCMetrcisBasedMetrics {

  public ProcedureMetrics(final String accountId,
                          final String applicationId,
                          final String procedureId,
                          final String pid,
                          final int instanceId) {
    super(String.format("%s.%s.%s.%s.%d",
                        accountId,
                        applicationId,
                        procedureId,
                        pid,
                        instanceId));
  }
}
