/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryService;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.continuuity.weave.discovery.InMemoryDiscoveryService;
import com.continuuity.weave.discovery.ZKDiscoveryService;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

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
      bind(DiscoveryServiceClient.class).to(ProcedureDiscoveryServiceClient.class);

      expose(DiscoveryService.class);
      expose(DiscoveryServiceClient.class);
    }
  }

  /**
   * A DiscoveryServiceClient implementation that will namespace correctly for procedure service discovery.
   * Otherwise it'll delegate to default one.
   */
  private static final class ProcedureDiscoveryServiceClient implements DiscoveryServiceClient {

    private static final Logger LOG = LoggerFactory.getLogger(ProcedureDiscoveryServiceClient.class);
    private static final long CACHE_EXPIRES_MINUTES = 1;

    private final ZKClient zkClient;
    private final DiscoveryServiceClient delegate;
    private final String weaveNamespace;
    private final LoadingCache<String, DiscoveryServiceClient> clients;

    @Inject
    private ProcedureDiscoveryServiceClient(ZKClient zkClient,
                                            CConfiguration configuration,
                                            @Named("local.discovery.client") DiscoveryServiceClient delegate) {
      this.zkClient = zkClient;
      this.delegate = delegate;
      this.weaveNamespace = configuration.get(Constants.CFG_WEAVE_ZK_NAMESPACE, "/weave");
      this.clients = CacheBuilder.newBuilder().expireAfterAccess(CACHE_EXPIRES_MINUTES, TimeUnit.MINUTES)
                                              .build(createClientLoader());
    }

    @Override
    public Iterable<Discoverable> discover(final String name) {
      if (!name.startsWith("procedure.")) {
        return delegate.discover(name);
      }

      return clients.getUnchecked(name).discover(name);
    }

    private CacheLoader<String, DiscoveryServiceClient> createClientLoader() {
      return new CacheLoader<String, DiscoveryServiceClient>() {
        @Override
        public DiscoveryServiceClient load(String key) throws Exception {
          String ns = weaveNamespace + "/PROCEDURE." + key.substring("procedure.".length());
          LOG.debug("Create ZKDiscoveryClient for " + ns);
          return new ZKDiscoveryService(ZKClients.namespace(zkClient, ns));
        }
      };
    }
  }
}
