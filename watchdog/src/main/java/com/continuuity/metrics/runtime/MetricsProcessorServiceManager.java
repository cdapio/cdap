package com.continuuity.metrics.runtime;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManager;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;

/**
 * Metrics Processor Reactor Service Management in Distributed Mode.
 */
public class MetricsProcessorServiceManager extends AbstractDistributedReactorServiceManager {

  @Inject
  public MetricsProcessorServiceManager(TwillRunnerService twillRunnerService) {
    super(Constants.Service.METRICS_PROCESSOR, twillRunnerService);
  }
}
