/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.guice;

import com.continuuity.api.io.SchemaGenerator;
import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.app.services.AppFabricServerFactory;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.authorization.PassportAuthorizationFactory;
import com.continuuity.internal.app.deploy.SyncManagerFactory;
import com.continuuity.internal.app.runtime.FlowletProgramRunner;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.services.InMemoryAppFabricServerFactory;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.pipeline.PipelineFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

/**
 *
 */
public class BigMamaModule extends AbstractModule {

  /**
   * Configures a {@link com.google.inject.Binder} via the exposed methods.
   */
  @Override
  protected void configure() {
    bind(ProgramRunnerFactory.class).to(InMemoryProgramRunnerFactory.class);
    bind(SchemaGenerator.class).to(ReflectionSchemaGenerator.class);
    bind(AppFabricServerFactory.class).to(InMemoryAppFabricServerFactory.class);
    bind(LocationFactory.class).to(LocalLocationFactory.class);
    bind(new TypeLiteral<PipelineFactory<?>>(){}).to(new TypeLiteral<SynchronousPipelineFactory<?>>(){});
    bind(ManagerFactory.class).to(SyncManagerFactory.class);
    bind(StoreFactory.class).to(MDSStoreFactory.class);
    bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
    bind(AuthorizationFactory.class).to(PassportAuthorizationFactory.class);
    bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
  }

  @Singleton
  private static final class InMemoryProgramRunnerFactory implements ProgramRunnerFactory {
    private final Injector injector;
    @Inject
    private InMemoryProgramRunnerFactory(Injector injector) {
      this.injector = injector;
    }

    @Override
    public ProgramRunner create() {
      return injector.getInstance(FlowletProgramRunner.class);
    }
  }
}
