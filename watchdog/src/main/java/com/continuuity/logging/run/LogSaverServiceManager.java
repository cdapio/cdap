package com.continuuity.logging.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManager;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;

/**
 * Log Saver Reactor Service Management in Distributed Mode.
 */
public class LogSaverServiceManager extends AbstractDistributedReactorServiceManager {

  @Inject
  public LogSaverServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService) {
    super(cConf, Constants.Service.LOGSAVER, twillRunnerService);
  }

  @Override
  public int getMaxInstances() {
    return cConf.getInt(Constants.LogSaver.MAX_INSTANCES);
  }

  @Override
  public boolean canCheckStatus() {
    return false;
  }

  @Override
  public boolean isServiceAvailable() {
    return true;
  }

  @Override
  public boolean isLogAvailable() {
    return false;
  }
}
