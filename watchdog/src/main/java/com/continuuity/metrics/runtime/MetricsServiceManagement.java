package com.continuuity.metrics.runtime;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManagement;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;

/**
 *
 */
public class MetricsServiceManagement extends AbstractDistributedReactorServiceManagement {

  @Inject
  public MetricsServiceManagement(TwillRunnerService twillRunnerService) {
    super(Constants.Service.METRICS, twillRunnerService);
  }
}
