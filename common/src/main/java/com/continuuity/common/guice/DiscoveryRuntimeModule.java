/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.guice;

import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.weave.discovery.DiscoveryService;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.continuuity.weave.discovery.InMemoryDiscoveryService;
import com.continuuity.weave.discovery.ZKDiscoveryService;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.name.Names;

/**
 * Provides Guice bindings for DiscoveryService and DiscoveryServiceClient for different
 * runtime environments.
 */
public final class DiscoveryRuntimeModule extends RuntimeModule {

  private final ZKClient zkClient;

  /**
   * Creates a {@link DiscoveryRuntimeModule} without ZKClient.
   * Used for InMemory and Singlenode mode.
   */
  public DiscoveryRuntimeModule() {
    this(null);
  }

  /**
   * Creates a {@link DiscoveryRuntimeModule} with the given ZKClient.
   * For Distributed modules, the given ZKClient will be used for
   * discovery purpose.
   */
  public DiscoveryRuntimeModule(ZKClient zkClient) {
    this.zkClient = zkClient;
  }

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
    Preconditions.checkArgument(zkClient != null, "No ZKClient provided.");
    return new ZKDiscoveryModule(zkClient);
  }

  private static final class InMemoryDiscoveryModule extends AbstractModule {

    @Override
    protected void configure() {
      InMemoryDiscoveryService discovery = new InMemoryDiscoveryService();
      bind(DiscoveryService.class).toInstance(discovery);
      bind(DiscoveryServiceClient.class).toInstance(discovery);
    }
  }

  private static final class ZKDiscoveryModule extends PrivateModule {

    private final ZKClient zkClient;
    private final ZKDiscoveryService discovery;

    private ZKDiscoveryModule(ZKClient zkClient) {
      this.zkClient = zkClient;
      this.discovery = new ZKDiscoveryService(zkClient);
    }

    @Override
    protected void configure() {
      bind(ZKClient.class).toInstance(zkClient);
      bind(DiscoveryService.class).toInstance(discovery);
      bind(DiscoveryServiceClient.class).annotatedWith(Names.named("local.discovery.client")).toInstance(discovery);
      bind(DiscoveryServiceClient.class).to(ProgramDiscoveryServiceClient.class);

      expose(DiscoveryService.class);
      expose(DiscoveryServiceClient.class);
    }
  }

}
