/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A DiscoveryServiceClient implementation that will namespace correctly for program service discovery.
 * Otherwise it'll delegate to default one.
 */
final class ProgramDiscoveryServiceClient implements DiscoveryServiceClient {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramDiscoveryServiceClient.class);
  private static final long CACHE_EXPIRES_MINUTES = 1;

  /**
   * Defines the set of program types that are discoverable.
   * TODO: We have program type in app-fabric, but here is common. Worth to refactor Type into one place.
   */
  private enum DiscoverableProgramType {
    PROCEDURE,
    WORKFLOW,
    WEBAPP;

    private final String prefix;

    DiscoverableProgramType() {
      prefix = name().toLowerCase() + ".";
    }

    boolean isPrefixOf(String name) {
      return name.startsWith(prefix);
    }
  }

  private final ZKClient zkClient;
  private final DiscoveryServiceClient delegate;
  private final String twillNamespace;
  private final LoadingCache<String, DiscoveryServiceClient> clients;

  @Inject
  ProgramDiscoveryServiceClient(ZKClient zkClient,
                                CConfiguration configuration,
                                @Named("local.discovery.client") DiscoveryServiceClient delegate) {
    this.zkClient = zkClient;
    this.delegate = delegate;
    this.twillNamespace = configuration.get(Constants.CFG_TWILL_ZK_NAMESPACE, "/weave");
    this.clients = CacheBuilder.newBuilder().expireAfterAccess(CACHE_EXPIRES_MINUTES, TimeUnit.MINUTES)
                                            .build(createClientLoader());
  }

  @Override
  public ServiceDiscovered discover(final String name) {
    for (DiscoverableProgramType type : DiscoverableProgramType.values()) {
      if (type.isPrefixOf(name)) {
        return clients.getUnchecked(name).discover(name);
      }
    }
    return delegate.discover(name);
  }

  private CacheLoader<String, DiscoveryServiceClient> createClientLoader() {
    return new CacheLoader<String, DiscoveryServiceClient>() {
      @Override
      public DiscoveryServiceClient load(String key) throws Exception {
        int idx = key.indexOf('.');  // It must be found as checked in the discover method
        String ns = String.format("%s/%s%s", twillNamespace, key.substring(0, idx), key.substring(idx));
        LOG.debug("Create ZKDiscoveryClient for " + ns);
        return new ZKDiscoveryService(ZKClients.namespace(zkClient, ns));
      }
    };
  }
}
