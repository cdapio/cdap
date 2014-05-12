package com.continuuity.security.guice;

import com.continuuity.common.runtime.RuntimeModule;
import com.google.inject.Module;

/**
 * Security guice modules
 */
//TODO: we need to have seperate implementations for inMemoryModule and singleNodeModule
public class SecurityModules extends RuntimeModule {
  @Override
  public Module getInMemoryModules() {
    return new InMemorySecurityModule();
  }

  @Override
  public Module getSingleNodeModules() {
    return new InMemorySecurityModule();
  }

  @Override
  public Module getDistributedModules() {
    return new DistributedSecurityModule();
  }
}
