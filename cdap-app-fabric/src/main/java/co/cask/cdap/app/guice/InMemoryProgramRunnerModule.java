/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.app.guice;

import co.cask.cdap.api.data.stream.StreamWriter;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.stream.DefaultStreamWriter;
import co.cask.cdap.app.stream.StreamWriterFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.logging.common.LocalLogWriter;
import co.cask.cdap.common.logging.common.LogWriter;
import co.cask.cdap.internal.app.queue.QueueReaderFactory;
import co.cask.cdap.internal.app.runtime.batch.MapReduceProgramRunner;
import co.cask.cdap.internal.app.runtime.flow.FlowProgramRunner;
import co.cask.cdap.internal.app.runtime.flow.FlowletProgramRunner;
import co.cask.cdap.internal.app.runtime.service.InMemoryProgramRuntimeService;
import co.cask.cdap.internal.app.runtime.service.InMemoryServiceProgramRunner;
import co.cask.cdap.internal.app.runtime.service.ServiceProgramRunner;
import co.cask.cdap.internal.app.runtime.spark.SparkProgramRunner;
import co.cask.cdap.internal.app.runtime.webapp.IntactJarHttpHandler;
import co.cask.cdap.internal.app.runtime.webapp.JarHttpHandler;
import co.cask.cdap.internal.app.runtime.webapp.WebappHttpHandlerFactory;
import co.cask.cdap.internal.app.runtime.webapp.WebappProgramRunner;
import co.cask.cdap.internal.app.runtime.worker.InMemoryWorkerRunner;
import co.cask.cdap.internal.app.runtime.worker.WorkerProgramRunner;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramRunner;
import co.cask.cdap.proto.ProgramType;
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
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public final class InMemoryProgramRunnerModule extends PrivateModule {

  private final Class<? extends StreamWriter> streamWriterClass;

  public InMemoryProgramRunnerModule() {
    this(null);
  }

  public InMemoryProgramRunnerModule(@Nullable Class<? extends StreamWriter> streamWriterClass) {
    if (streamWriterClass == null) {
      this.streamWriterClass = DefaultStreamWriter.class;
    } else {
      this.streamWriterClass = streamWriterClass;
    }
  }

  /**
   * Configures a {@link com.google.inject.Binder} via the exposed methods.
   */
  @Override
  protected void configure() {

    // Bind and expose LogWriter (a bit hacky, but needed by MapReduce for now)
    bind(LogWriter.class).to(LocalLogWriter.class);
    expose(LogWriter.class);

    // Bind ServiceAnnouncer for service.
    bind(ServiceAnnouncer.class).to(DiscoveryServiceAnnouncer.class);

    // For Binding queue stuff
    bind(QueueReaderFactory.class).in(Scopes.SINGLETON);

    // Bind ProgramRunner
    MapBinder<ProgramType, ProgramRunner> runnerFactoryBinder =
      MapBinder.newMapBinder(binder(), ProgramType.class, ProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramType.FLOW).to(FlowProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramType.MAPREDUCE).to(MapReduceProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramType.SPARK).to(SparkProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramType.WORKFLOW).to(WorkflowProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramType.WEBAPP).to(WebappProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramType.WORKER).to(InMemoryWorkerRunner.class);
    runnerFactoryBinder.addBinding(ProgramType.SERVICE).to(InMemoryServiceProgramRunner.class);

    // Bind these three program runner in private scope
    // They should only be used by the ProgramRunners in the runnerFactoryBinder
    bind(FlowletProgramRunner.class);
    bind(ServiceProgramRunner.class);
    bind(WorkerProgramRunner.class);

    bind(ProgramRunnerFactory.class).to(InMemoryFlowProgramRunnerFactory.class).in(Scopes.SINGLETON);
    // Note: Expose for test cases. Need to refactor test cases.
    expose(ProgramRunnerFactory.class);

    // Bind and expose runtime service
    bind(ProgramRuntimeService.class).to(InMemoryProgramRuntimeService.class).in(Scopes.SINGLETON);
    expose(ProgramRuntimeService.class);

    // For binding DataSet transaction stuff
    install(new DataFabricFacadeModule());

    // Create webapp http handler factory.
    install(new FactoryModuleBuilder().implement(JarHttpHandler.class, IntactJarHttpHandler.class)
              .build(WebappHttpHandlerFactory.class));

    // Create StreamWriter factory.
    install(new FactoryModuleBuilder().implement(StreamWriter.class, streamWriterClass)
              .build(StreamWriterFactory.class));
  }

  @Singleton
  @Provides
  private LocalLogWriter providesLogWriter(CConfiguration configuration) {
    return new LocalLogWriter(configuration);
  }

  @Singleton
  private static final class InMemoryFlowProgramRunnerFactory implements ProgramRunnerFactory {

    private final Map<ProgramType, Provider<ProgramRunner>> providers;

    @Inject
    private InMemoryFlowProgramRunnerFactory(Map<ProgramType, Provider<ProgramRunner>> providers) {
      this.providers = providers;
    }

    @Override
    public ProgramRunner create(ProgramType programType) {
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
      return discoveryService.register(ResolvingDiscoverable.of(new Discoverable() {
        @Override
        public String getName() {
          return serviceName;
        }

        @Override
        public InetSocketAddress getSocketAddress() {
          return new InetSocketAddress(hostname, port);
        }
      }));
    }
  }
}
