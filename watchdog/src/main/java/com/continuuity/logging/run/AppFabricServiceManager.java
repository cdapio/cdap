package com.continuuity.logging.run;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractInMemoryReactorServiceManager;

/**
 * Service for managing app fabric service.
 */
public class AppFabricServiceManager extends AbstractInMemoryReactorServiceManager {

  @Override
  public String getDescription() {
    return Constants.AppFabric.SERVICE_DESCRIPTION;
  }
}
