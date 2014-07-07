package com.continuuity.explore.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManager;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Service manager for explore service in distributed mode.
 */
public class ExploreServiceManager extends AbstractDistributedReactorServiceManager {

  @Inject
  public ExploreServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                              DiscoveryServiceClient discoveryServiceClient) {
    super(cConf, Constants.Service.EXPLORE_HTTP_USER_SERVICE, twillRunnerService, discoveryServiceClient);
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public boolean isServiceEnabled() {
    return cConf.getBoolean(Constants.Explore.CFG_EXPLORE_ENABLED);
  }

  @Override
  public int getMaxInstances() {
    return cConf.getInt(Constants.Explore.MAX_INSTANCES, 1);
  }

  @Override
  public String getDescription() {
    return Constants.Explore.SERVICE_DESCRIPTION;
  }

}
