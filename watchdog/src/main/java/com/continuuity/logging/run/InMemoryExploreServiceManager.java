package com.continuuity.logging.run;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractInMemoryReactorServiceManager;

/**
 * In memory explore service manager.
 */
public class InMemoryExploreServiceManager extends AbstractInMemoryReactorServiceManager {

  @Override
  public String getDescription() {
    return Constants.Explore.SERVICE_DESCRIPTION;
  }
}
