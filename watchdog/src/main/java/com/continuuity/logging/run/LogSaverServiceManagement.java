package com.continuuity.logging.run;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManagement;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;

/**
 * Log Saver Reactor Service Management in Distributed Mode.
 */
public class LogSaverServiceManagement extends AbstractDistributedReactorServiceManagement {

  @Inject
  public LogSaverServiceManagement(TwillRunnerService twillRunnerService) {
    super(Constants.Service.LOGSAVER, twillRunnerService);
  }
}
