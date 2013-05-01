/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.guice;

import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.internal.discovery.InMemoryDiscoveryService;
import com.continuuity.internal.discovery.ZKDiscoveryService;
import com.continuuity.zookeeper.DiscoveryService;
import com.continuuity.zookeeper.DiscoveryServiceClient;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;

/**
 * A {@link RuntimeModule} that defines different guice modules
 * for {@link DiscoveryService} and {@link DiscoveryServiceClient}.
 */
public final class DiscoveryServiceModules extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return moduleOf(InMemoryDiscoveryService.class);
  }

  @Override
  public Module getSingleNodeModules() {
    return moduleOf(InMemoryDiscoveryService.class);
  }

  @Override
  public Module getDistributedModules() {
    return moduleOf(ZKDiscoveryService.class);
  }

  private <T extends DiscoveryService & DiscoveryServiceClient> DiscoveryServiceModule<T> moduleOf(Class<T> clz) {
    return new DiscoveryServiceModule<T>(clz);
  }

  /**
   * A Generic {@link PrivateModule} that binds {@link DiscoveryService} and {@link DiscoveryServiceClient} to the
   * same singleton class of type {@code T}.
   * @param <T> Type of the implementation class. The implementation needs to implements both {@link DiscoveryService}
   *           and {@link DiscoveryServiceClient}.
   */
  private static final class DiscoveryServiceModule<T extends DiscoveryService &
                                                              DiscoveryServiceClient> extends PrivateModule {
    private final Class<T> implClass;

    private DiscoveryServiceModule(Class<T> implClass) {
      this.implClass = implClass;
    }

    @Override
    protected void configure() {
      bind(implClass).in(Scopes.SINGLETON);
      bind(DiscoveryService.class).to(implClass);
      bind(DiscoveryServiceClient.class).to(implClass);

      expose(DiscoveryService.class);
      expose(DiscoveryServiceClient.class);
    }
  }
}
