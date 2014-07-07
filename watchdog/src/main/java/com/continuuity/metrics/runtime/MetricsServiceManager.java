package com.continuuity.metrics.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManager;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Metrics Reactor Service Management in Distributed Mode.
 */
public class MetricsServiceManager extends AbstractDistributedReactorServiceManager {

  @Inject
  public MetricsServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                               DiscoveryServiceClient discoveryServiceClient) {
    super(cConf, Constants.Service.METRICS, twillRunnerService, discoveryServiceClient);
  }

  @Override
  public int getMaxInstances() {
    return cConf.getInt(Constants.Metrics.MAX_INSTANCES);
  }

  @Override
  public String getDescription() {
    return Constants.Metrics.SERVICE_DESCRIPTION;
  }

}
