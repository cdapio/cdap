package com.continuuity.metrics.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManager;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;

/**
 * Metrics Processor Reactor Service Management in Distributed Mode.
 */
public class MetricsProcessorServiceManager extends AbstractDistributedReactorServiceManager {

  @Inject
  public MetricsProcessorServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService) {
    super(cConf, Constants.Service.METRICS_PROCESSOR, twillRunnerService);
  }

  @Override
  public int getMaxInstances() {
    return cConf.getInt(Constants.MetricsProcessor.MAX_INSTANCES);
  }

  @Override
  public boolean canCheckStatus() {
    return false;
  }

  @Override
  public boolean isServiceAvailable() {
    return true;
  }
}
