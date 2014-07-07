package com.continuuity.logging.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManager;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Log Saver Reactor Service Management in Distributed Mode.
 */
public class LogSaverStatusServiceManager extends AbstractDistributedReactorServiceManager {

  @Inject
  public LogSaverStatusServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                                      DiscoveryServiceClient discoveryServiceClient) {
    super(cConf, Constants.Service.LOGSAVER, twillRunnerService, discoveryServiceClient);
  }

  @Override
  public int getMaxInstances() {
    return cConf.getInt(Constants.LogSaver.MAX_INSTANCES);
  }

  @Override
  public boolean isLogAvailable() {
    return false;
  }

  @Override
  public String getDescription() {
    return Constants.LogSaver.SERVICE_DESCRIPTION;
  }
}
