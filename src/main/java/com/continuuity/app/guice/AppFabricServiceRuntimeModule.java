/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.guice;

import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.internal.app.authorization.PassportAuthorizationFactory;
import com.continuuity.internal.app.deploy.SyncManagerFactory;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.internal.app.services.DefaultAppFabricService;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.pipeline.PipelineFactory;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

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
  private static final class AppFabricServiceModule extends PrivateModule {

    @Override
    protected void configure() {
      bind(new TypeLiteral<PipelineFactory<?>>(){}).to(new TypeLiteral<SynchronousPipelineFactory<?>>(){});
      bind(ManagerFactory.class).to(SyncManagerFactory.class);

      bind(AuthorizationFactory.class).to(PassportAuthorizationFactory.class);
      bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
      bind(AppFabricService.Iface.class).to(DefaultAppFabricService.class);

      // Bind and expose MetaDataStore and StoreFactory.
      // Hacky as it's actually exposed for gateway. Should fix gateway to have it's own Guice module.
      bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
      expose(MetaDataStore.class);
      bind(StoreFactory.class).to(MDSStoreFactory.class);
      expose(StoreFactory.class);

      // Bind and expose thrift service AppFabricService.
      bind(AppFabricServer.class);
      expose(AppFabricServer.class);
    }
  }
}
