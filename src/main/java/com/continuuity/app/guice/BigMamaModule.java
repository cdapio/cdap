/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.guice;

import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.logging.common.LocalLogWriter;
import com.continuuity.common.logging.common.LogWriter;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.authorization.PassportAuthorizationFactory;
import com.continuuity.internal.app.deploy.SyncManagerFactory;
import com.continuuity.internal.app.queue.QueueReaderFactory;
import com.continuuity.internal.app.queue.SingleQueueReader;
import com.continuuity.internal.app.runtime.DataFabricFacade;
import com.continuuity.internal.app.runtime.DataFabricFacadeFactory;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.SmartDataFabricFacade;
import com.continuuity.internal.app.runtime.batch.MapReduceProgramRunner;
import com.continuuity.internal.app.runtime.batch.MapReduceRuntimeService;
import com.continuuity.internal.app.runtime.batch.inmemory.InMemoryMapReduceRuntimeService;
import com.continuuity.internal.app.runtime.flow.FlowProgramRunner;
import com.continuuity.internal.app.runtime.flow.FlowletProgramRunner;
import com.continuuity.internal.app.runtime.procedure.ProcedureProgramRunner;
import com.continuuity.internal.app.runtime.service.InMemoryProgramRuntimeService;
import com.continuuity.internal.app.services.DefaultAppFabricService;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.SchemaGenerator;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.pipeline.PipelineFactory;
import com.continuuity.weave.api.ServiceAnnouncer;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryService;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.continuuity.weave.discovery.InMemoryDiscoveryService;
import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 *
 */
public class BigMamaModule extends AbstractModule {

  private final CConfiguration configuration;

  public BigMamaModule(CConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Configures a {@link com.google.inject.Binder} via the exposed methods.
   */
  @Override
  protected void configure() {

    String configHostname = configuration.get(Constants.CFG_APP_FABRIC_SERVER_ADDRESS);
    InetAddress hostname;
    try {
      if (configHostname != null) {
        hostname = InetAddress.getByName(configHostname);
      } else {
        hostname = InetAddress.getLocalHost();
      }
    } catch (UnknownHostException e) {
      hostname = new InetSocketAddress("localhost", 0).getAddress();
    }

    bind(InetAddress.class).annotatedWith(Names.named("config.hostname")).toInstance(hostname);

    // Bind config
    bind(CConfiguration.class).toInstance(configuration);

    // Bind LogWriter
    bind(LogWriter.class).toInstance(new LocalLogWriter(configuration));

    // Bind Discovery service
    bind(InMemoryDiscoveryService.class).in(Scopes.SINGLETON);
    bind(DiscoveryService.class).to(InMemoryDiscoveryService.class);
    bind(DiscoveryServiceClient.class).to(InMemoryDiscoveryService.class);
    bind(ServiceAnnouncer.class).to(DiscoveryServiceAnnouncer.class);

    // Bind ProgramRunner
    MapBinder<ProgramRunnerFactory.Type, ProgramRunner> runnerFactoryBinder =
      MapBinder.newMapBinder(binder(), ProgramRunnerFactory.Type.class, ProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.FLOW).to(FlowProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.FLOWLET).to(FlowletProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.PROCEDURE).to(ProcedureProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.MAPREDUCE).to(MapReduceProgramRunner.class);

    bind(ProgramRunnerFactory.class).to(InMemoryFlowProgramRunnerFactory.class).in(Scopes.SINGLETON);

    // Bind runtime service
    bind(ProgramRuntimeService.class).to(InMemoryProgramRuntimeService.class).in(Scopes.SINGLETON);

    // Bind MapReduce runtime service
    bind(MapReduceRuntimeService.class).to(InMemoryMapReduceRuntimeService.class).in(Scopes.SINGLETON);

    bind(SchemaGenerator.class).to(ReflectionSchemaGenerator.class);

    bind(LocationFactory.class).to(LocalLocationFactory.class);
    bind(new TypeLiteral<PipelineFactory<?>>(){}).to(new TypeLiteral<SynchronousPipelineFactory<?>>(){});
    bind(ManagerFactory.class).to(SyncManagerFactory.class);
    bind(StoreFactory.class).to(MDSStoreFactory.class);
    bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
    bind(AuthorizationFactory.class).to(PassportAuthorizationFactory.class);
    bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
    bind(AppFabricService.Iface.class).to(DefaultAppFabricService.class);

    // For binding DataSet transaction stuff
    install(new PrivateModule() {
      @Override
      protected void configure() {
        install(new FactoryModuleBuilder()
                .implement(DataFabricFacade.class, SmartDataFabricFacade.class)
                .build(DataFabricFacadeFactory.class));

        expose(DataFabricFacadeFactory.class);
      }
    });

    // For Binding queue stuff
    install(new FactoryModuleBuilder()
            .implement(QueueReader.class, SingleQueueReader.class)
            .build(QueueReaderFactory.class));

    // For binding IO stuff
    install(new IOModule());
  }


  @Singleton
  private static final class InMemoryFlowProgramRunnerFactory implements ProgramRunnerFactory {

    private final Map<ProgramRunnerFactory.Type, Provider<ProgramRunner>> providers;

    @Inject
    private InMemoryFlowProgramRunnerFactory(Map<ProgramRunnerFactory.Type, Provider<ProgramRunner>> providers) {
      this.providers = providers;
    }

    @Override
    public ProgramRunner create(ProgramRunnerFactory.Type programType) {
      Provider<ProgramRunner> provider = providers.get(programType);
      Preconditions.checkNotNull(provider, "Unsupported program type: " + programType);
      return provider.get();
    }
  }

  @Singleton
  private static final class DiscoveryServiceAnnouncer implements ServiceAnnouncer {

    private final DiscoveryService discoveryService;
    private final InetAddress hostname;

    @Inject
    private DiscoveryServiceAnnouncer(DiscoveryService discoveryService,
                                      @Named("config.hostname") InetAddress hostname) {
      this.discoveryService = discoveryService;
      this.hostname = hostname;
    }

    @Override
    public Cancellable announce(final String serviceName, final int port) {
      return discoveryService.register(new Discoverable() {
        @Override
        public String getName() {
          return serviceName;
        }

        @Override
        public InetSocketAddress getSocketAddress() {
          return new InetSocketAddress(hostname, port);
        }
      });
    }
  }
}
