package com.continuuity.metrics.runtime;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManager;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;

/**
 * Metrics Reactor Service Management in Distributed Mode.
 */
public class MetricsServiceManager extends AbstractDistributedReactorServiceManager {

  @Inject
  public MetricsServiceManager(TwillRunnerService twillRunnerService) {
    super(Constants.Service.METRICS, twillRunnerService);
  }
}
