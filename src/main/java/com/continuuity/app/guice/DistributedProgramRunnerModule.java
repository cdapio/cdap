/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.guice;

import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.batch.MapReduceProgramRunner;
import com.continuuity.internal.app.runtime.distributed.DistributedFlowProgramRunner;
import com.continuuity.internal.app.runtime.distributed.DistributedProcedureProgramRunner;
import com.continuuity.internal.app.runtime.distributed.DistributedProgramRuntimeService;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryService;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.continuuity.weave.discovery.ZKDiscoveryService;
import com.continuuity.weave.yarn.YarnWeaveRunnerService;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Guice module for distributed AppFabric. Used by the app-fabric server, not for distributed containers.
 */
final class DistributedProgramRunnerModule extends PrivateModule {

  private final ZKClient zkClient;

  DistributedProgramRunnerModule(ZKClient zkClient) {
    this.zkClient = zkClient;
  }

  @Override
  protected void configure() {
    // Bind ZKClient
    bind(ZKClient.class).toInstance(zkClient);

    // Bind and expose WeaveRunner
    bindConstant().annotatedWith(Names.named("config.weave.namespace")).to("/weave");
    bind(WeaveRunner.class).to(YarnWeaveRunnerService.class);
    bind(WeaveRunnerService.class).to(YarnWeaveRunnerService.class);

    expose(WeaveRunner.class);
    expose(WeaveRunnerService.class);

    // Bind and expose DiscoveryService
    bind(DiscoveryService.class).to(ZKDiscoveryService.class);
    expose(DiscoveryService.class);

    // Bind and expose DiscoveryServiceClient.
    bind(DiscoveryServiceClient.class)
      .annotatedWith(Names.named("local.discovery.client"))
      .to(ZKDiscoveryService.class);
    bind(DiscoveryServiceClient.class).to(ProcedureDiscoveryServiceClient.class);
    expose(DiscoveryServiceClient.class);

    // Bind ProgramRunner
    MapBinder<ProgramRunnerFactory.Type, ProgramRunner> runnerFactoryBinder =
      MapBinder.newMapBinder(binder(), ProgramRunnerFactory.Type.class, ProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.FLOW).to(DistributedFlowProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.PROCEDURE).to(DistributedProcedureProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.MAPREDUCE).to(MapReduceProgramRunner.class);

    // Bind and expose ProgramRuntimeService
    bind(ProgramRuntimeService.class).to(DistributedProgramRuntimeService.class);
    expose(ProgramRuntimeService.class);
  }

  /**
   * Singleton provider for {@link ZKDiscoveryService}.
   */
  @Singleton
  @Provides
  private ZKDiscoveryService provideZKDiscoveryService(ZKClient zkClient) {
    return new ZKDiscoveryService(zkClient);
  }

  @Singleton
  @Provides
  private YarnWeaveRunnerService provideYarnWeaveRunnerService(@Named("config.weave.namespace") String namespace,
                                                               CConfiguration configuration,
                                                               YarnConfiguration yarnConfiguration) {
    return new YarnWeaveRunnerService(yarnConfiguration, configuration.get("zookeeper.quorum") + namespace);
  }

  @Singleton
  @Provides
  private ProgramRunnerFactory provideProgramRunnerFactory(final Map<ProgramRunnerFactory.Type,
                                                                     Provider<ProgramRunner>> providers) {

    return new ProgramRunnerFactory() {
      @Override
      public ProgramRunner create(Type programType) {
        Provider<ProgramRunner> provider = providers.get(programType);
        Preconditions.checkNotNull(provider, "Unsupported program type: " + programType);
        return provider.get();
      }
    };
  }

  /**
   * A DiscoveryServiceClient implementation that will namespace correctly for procedure service discovery.
   * Otherwise it'll delegate to default one.
   */
  private static final class ProcedureDiscoveryServiceClient implements DiscoveryServiceClient {

    private static final Logger LOG = LoggerFactory.getLogger(ProcedureDiscoveryServiceClient.class);

    private final ZKClient zkClient;
    private final DiscoveryServiceClient delegate;
    private final String weaveNamespace;
    private final ConcurrentMap<String, DiscoveryServiceClient> clients;

    @Inject
    private ProcedureDiscoveryServiceClient(ZKClient zkClient,
                                            @Named("local.discovery.client") DiscoveryServiceClient delegate,
                                            @Named("config.weave.namespace") String namespace) {
      this.zkClient = zkClient;
      this.delegate = delegate;
      this.weaveNamespace = namespace;
      this.clients = Maps.newConcurrentMap();
    }

    @Override
    public Iterable<Discoverable> discover(final String name) {
      if (!name.startsWith("procedure.")) {
        return delegate.discover(name);
      }

      String ns = weaveNamespace + "/" + name;
      LOG.info("Discover: " + ns + ", " + name);
      DiscoveryServiceClient client = new ZKDiscoveryService(ZKClients.namespace(zkClient, ns));
      DiscoveryServiceClient oldClient = clients.putIfAbsent(name, client);
      if (oldClient != null) {
        client = oldClient;
      }
      final Iterable<Discoverable> result = client.discover(name);

      return new Iterable<Discoverable>() {

        @Override
        public Iterator<Discoverable> iterator() {
          LOG.info("Discovered: " + name + " " + Iterables.toString(result));
          return result.iterator();
        }
      };
    }
  }
}
