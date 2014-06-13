package com.continuuity.hive.guice;

import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.explore.service.ExploreService;
import com.continuuity.explore.service.hive.Hive13ExploreService;

import com.google.inject.AbstractModule;
import com.google.inject.Module;

/**
 * Guice runtime module for the explore functionality.
 */
public class ExploreRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new ExploreInMemoryModule();
  }

  @Override
  public Module getSingleNodeModules() {
    return null;
  }

  @Override
  public Module getDistributedModules() {
    return null;
  }

  private static final class ExploreInMemoryModule extends AbstractModule {

    @Override
    protected void configure() {
      // Current version of hive used in Singlenode is Hive 13
      bind(ExploreService.class).to(Hive13ExploreService.class);
    }
  }
}
