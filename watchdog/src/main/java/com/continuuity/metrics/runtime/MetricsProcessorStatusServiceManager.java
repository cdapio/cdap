package com.continuuity.metrics.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManager;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Metrics Processor Reactor Service Management in Distributed Mode.
 */
public class MetricsProcessorStatusServiceManager extends AbstractDistributedReactorServiceManager {

  @Inject
  public MetricsProcessorStatusServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                                              DiscoveryServiceClient discoveryServiceClient) {
    super(cConf, Constants.Service.METRICS_PROCESSOR, twillRunnerService, discoveryServiceClient);
  }

  @Override
  public int getMaxInstances() {
    return cConf.getInt(Constants.MetricsProcessor.MAX_INSTANCES);
  }

  @Override
  public String getDescription() {
    return Constants.MetricsProcessor.SERVICE_DESCRIPTION;
  }

}
