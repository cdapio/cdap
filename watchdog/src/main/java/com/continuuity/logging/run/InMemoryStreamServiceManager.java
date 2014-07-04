package com.continuuity.logging.run;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractInMemoryReactorServiceManager;

/**
 * In memory manager for stream service.
 */
public class InMemoryStreamServiceManager extends AbstractInMemoryReactorServiceManager {

  @Override
  public String getDescription() {
    return Constants.Stream.SERVICE_DESCRIPTION;
  }
}
