package com.continuuity.logging.run;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManager;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;

/**
 * Log Saver Reactor Service Management in Distributed Mode.
 */
public class LogSaverServiceManager extends AbstractDistributedReactorServiceManager {

  @Inject
  public LogSaverServiceManager(TwillRunnerService twillRunnerService) {
    super(Constants.Service.LOGSAVER, twillRunnerService);
  }
}
