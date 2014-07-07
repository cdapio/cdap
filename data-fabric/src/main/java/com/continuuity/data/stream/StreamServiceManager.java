package com.continuuity.data.stream;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManager;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Stream Reactor Service management in distributed mode.
 */
public class StreamServiceManager extends AbstractDistributedReactorServiceManager {


  @Inject
  public StreamServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                              DiscoveryServiceClient discoveryServiceClient) {
    super(cConf, Constants.Service.STREAMS, twillRunnerService, discoveryServiceClient);
  }

  @Override
  public int getMaxInstances() {
    return cConf.getInt(Constants.Stream.MAX_INSTANCES);
  }

  @Override
  public String getDescription() {
    return Constants.Stream.SERVICE_DESCRIPTION;
  }

}
