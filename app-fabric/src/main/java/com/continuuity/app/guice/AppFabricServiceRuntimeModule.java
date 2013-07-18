/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.guice;

import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.internal.app.authorization.PassportAuthorizationFactory;
import com.continuuity.internal.app.deploy.SyncManagerFactory;
import com.continuuity.internal.app.services.DefaultAppFabricService;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.pipeline.PipelineFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 *
 */
public final class AppFabricServiceRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new AppFabricServiceModule();
  }

  @Override
  public Module getSingleNodeModules() {
    return new AppFabricServiceModule();
  }

  @Override
  public Module getDistributedModules() {
    return new AppFabricServiceModule();
  }

  /**
   * Guice module for AppFabricServer. Requires Opex related bindings being available.
   */
  // Note: Ideally this should be private module, but gateway and test cases uses some of the internal bindings.
  private static final class AppFabricServiceModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(new TypeLiteral<PipelineFactory<?>>(){}).to(new TypeLiteral<SynchronousPipelineFactory<?>>(){});
      bind(ManagerFactory.class).to(SyncManagerFactory.class);

      bind(AuthorizationFactory.class).to(PassportAuthorizationFactory.class);
      bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
      bind(AppFabricService.Iface.class).to(DefaultAppFabricService.class);

      bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
      bind(StoreFactory.class).to(MDSStoreFactory.class);
    }

    @Provides
    @Named(Constants.CFG_APP_FABRIC_SERVER_ADDRESS)
    public InetAddress providesHostname(CConfiguration cConf) {
      return Networks.resolve(cConf.get(Constants.CFG_APP_FABRIC_SERVER_ADDRESS),
                              new InetSocketAddress("localhost", 0).getAddress());
    }
  }
}
