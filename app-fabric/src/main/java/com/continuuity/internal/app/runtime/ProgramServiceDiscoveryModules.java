package com.continuuity.internal.app.runtime;

import com.continuuity.internal.app.runtime.distributed.DistributedProgramServiceDiscovery;
import com.google.inject.AbstractModule;
import com.google.inject.Module;

/**
 *
 */
public class ProgramServiceDiscoveryModules {
  public Module getDistributedModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(ProgramServiceDiscovery.class).to(DistributedProgramServiceDiscovery.class);
      }
    };
  }
}
