package com.continuuity.metrics.runtime;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManagement;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;

/**
 *
 */
public class MetricsProcessorServiceManagement extends AbstractDistributedReactorServiceManagement {

  @Inject
  public MetricsProcessorServiceManagement(TwillRunnerService twillRunnerService) {
    super(Constants.Service.METRICS_PROCESSOR, twillRunnerService);
  }
}
