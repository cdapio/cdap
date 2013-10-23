/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.guice;

import com.continuuity.app.queue.QueueReader;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.common.logging.common.LocalLogWriter;
import com.continuuity.common.logging.common.LogWriter;
import com.continuuity.internal.app.queue.QueueReaderFactory;
import com.continuuity.internal.app.queue.SingleQueue2Reader;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.batch.MapReduceProgramRunner;
import com.continuuity.internal.app.runtime.flow.FlowProgramRunner;
import com.continuuity.internal.app.runtime.flow.FlowletProgramRunner;
import com.continuuity.internal.app.runtime.procedure.ProcedureProgramRunner;
import com.continuuity.internal.app.runtime.service.InMemoryProgramRuntimeService;
import com.continuuity.internal.app.runtime.webapp.IntactJarHttpHandler;
import com.continuuity.internal.app.runtime.webapp.WebappHttpHandlerFactory;
import com.continuuity.internal.app.runtime.webapp.WebappProgramRunner;
import com.continuuity.internal.app.runtime.workflow.WorkflowProgramRunner;
import com.continuuity.weave.api.ServiceAnnouncer;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryService;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;

/**
 *
 */
final class InMemoryProgramRunnerModule extends PrivateModule {

  /**
   * Configures a {@link com.google.inject.Binder} via the exposed methods.
   */
  @Override
  protected void configure() {

    // Bind and expose LogWriter (a bit hacky, but needed by MapReduce for now)
    bind(LogWriter.class).to(LocalLogWriter.class);
    expose(LogWriter.class);

    // Bind ServiceAnnouncer for procedure.
    bind(ServiceAnnouncer.class).to(DiscoveryServiceAnnouncer.class);

    // Bind ProgramRunner
    MapBinder<ProgramRunnerFactory.Type, ProgramRunner> runnerFactoryBinder =
      MapBinder.newMapBinder(binder(), ProgramRunnerFactory.Type.class, ProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.FLOW).to(FlowProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.FLOWLET).to(FlowletProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.PROCEDURE).to(ProcedureProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.MAPREDUCE).to(MapReduceProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.WORKFLOW).to(WorkflowProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.WEBAPP).to(WebappProgramRunner.class);

    bind(ProgramRunnerFactory.class).to(InMemoryFlowProgramRunnerFactory.class).in(Scopes.SINGLETON);
    // Note: Expose for test cases. Need to refactor test cases.
    expose(ProgramRunnerFactory.class);

    // Bind and expose runtime service
    bind(ProgramRuntimeService.class).to(InMemoryProgramRuntimeService.class).in(Scopes.SINGLETON);
    expose(ProgramRuntimeService.class);

    // For binding DataSet transaction stuff
    install(new DataFabricFacadeModule());

    // For Binding queue stuff
    install(new FactoryModuleBuilder()
            .implement(QueueReader.class, SingleQueue2Reader.class)
            .build(QueueReaderFactory.class));

    // Create webapp http handler factory.
    install(new FactoryModuleBuilder().implement(HttpHandler.class, IntactJarHttpHandler.class)
              .build(WebappHttpHandlerFactory.class));
  }

  @Singleton
  @Provides
  private LocalLogWriter providesLogWriter(CConfiguration configuration) {
    return new LocalLogWriter(configuration);
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
                                      @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname) {
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
