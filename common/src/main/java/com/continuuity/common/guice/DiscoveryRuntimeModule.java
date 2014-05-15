/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.guice;

import com.continuuity.common.runtime.RuntimeModule;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.zookeeper.ZKClient;

/**
 * Provides Guice bindings for DiscoveryService and DiscoveryServiceClient for different
 * runtime environments.
 */
public final class DiscoveryRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new InMemoryDiscoveryModule();
  }

  @Override
  public Module getSingleNodeModules() {
    return new InMemoryDiscoveryModule();
  }

  @Override
  public Module getDistributedModules() {
    return new ZKDiscoveryModule();
  }

  private static final class InMemoryDiscoveryModule extends AbstractModule {

    // ensuring to be singleton across JVM
    private static final InMemoryDiscoveryService IN_MEMORY_DISCOVERY_SERVICE = new InMemoryDiscoveryService();

    @Override
    protected void configure() {
      InMemoryDiscoveryService discovery = IN_MEMORY_DISCOVERY_SERVICE;
      bind(DiscoveryService.class).toInstance(discovery);
      bind(DiscoveryServiceClient.class).toInstance(discovery);
    }
  }

  private static final class ZKDiscoveryModule extends PrivateModule {

    @Override
    protected void configure() {
      bind(DiscoveryService.class).to(ZKDiscoveryService.class);
      bind(DiscoveryServiceClient.class)
        .annotatedWith(Names.named("local.discovery.client"))
        .to(ZKDiscoveryService.class);
      bind(DiscoveryServiceClient.class).to(ProgramDiscoveryServiceClient.class);

      expose(DiscoveryService.class);
      expose(DiscoveryServiceClient.class);
    }

    @Provides
    @Singleton
    private ZKDiscoveryService providesDiscoveryService(ZKClient zkClient) {
      return new ZKDiscoveryService(zkClient);
    }
  }
}
