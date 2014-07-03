package com.continuuity.logging.run;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractInMemoryReactorServiceManager;

/**
 *  In memory dataset executor service manager.
 */
public class InMemoryDatasetExecutorServiceManager extends AbstractInMemoryReactorServiceManager {

  @Override
  public String getDescription() {
    return Constants.Dataset.Executor.SERVICE_DESCRIPTION;
  }
}
