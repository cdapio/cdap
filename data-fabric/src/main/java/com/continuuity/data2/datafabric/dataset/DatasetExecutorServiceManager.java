package com.continuuity.data2.datafabric.dataset;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManager;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Dataset Reactor Service management in distributed mode.
 */
public class DatasetExecutorServiceManager extends AbstractDistributedReactorServiceManager {

  @Inject
  public DatasetExecutorServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                                       DiscoveryServiceClient discoveryServiceClient) {
    super(cConf, Constants.Service.DATASET_EXECUTOR, twillRunnerService, discoveryServiceClient);
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public int getMaxInstances() {
    return cConf.getInt(Constants.Dataset.Executor.MAX_INSTANCES);
  }

  @Override
  public String getDescription() {
    return Constants.Dataset.Executor.SERVICE_DESCRIPTION;
  }

}
