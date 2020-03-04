/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.launcher;

import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Launcher Discovery module.
 */
public class LauncherDiscoveryModule extends PrivateModule {
  public static final String DELEGATE_DISCOVERY_SERVICE = "delegateDiscoveryService";
  public static final String DELEGATE_DISCOVERY_SERVICE_CLIENT = "delegateDiscoveryServiceClient";

  @Override
  protected void configure() {
//    bind(ZKClientService.class).toProvider(ZKServiceProvider.class).in(Scopes.SINGLETON);
//    bind(ZKClient.class).to(ZKClientService.class);
//    bind(ZKDiscoveryService.class).toProvider(ZKDiscoveryServiceProvider.class).in(Scopes.SINGLETON);
//    expose(ZKClientService.class);
//
//    bind(DiscoveryService.class).annotatedWith(Names.named(DELEGATE_DISCOVERY_SERVICE))
//      .to(ZKDiscoveryService.class).in(Scopes.SINGLETON);
//    bind(DiscoveryServiceClient.class).annotatedWith(Names.named(DELEGATE_DISCOVERY_SERVICE_CLIENT))
//      .to(ProgramDiscoveryServiceClient.class).in(Scopes.SINGLETON);

    bind(LauncherDiscoveryService.class).in(Scopes.SINGLETON);
    bind(DiscoveryService.class).to(LauncherDiscoveryService.class);
    bind(DiscoveryServiceClient.class).to(LauncherDiscoveryService.class);
    expose(DiscoveryService.class);
    expose(DiscoveryServiceClient.class);
  }

//  /**
//   * A Guice Provider to provide instance of {@link ZKClientService}.
//   */
//  private static final class ZKServiceProvider implements Provider<ZKClientService> {
//
//    @Override
//    public ZKClientService get() {
//      return ZKClientServices.delegate(
//        ZKClients.reWatchOnExpire(
//          ZKClients.retryOnFailure(
//            ZKClientService.Builder.of("localhost:2181")
//              .setSessionTimeout(40000)
//              .build(),
//            RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
//          )
//        )
//      );
//    }
//  }
//
//  /**
//   * A Guice Provider to provide instance of {@link ZKDiscoveryService}.
//   */
//  private static final class ZKDiscoveryServiceProvider implements Provider<ZKDiscoveryService> {
//
//    private final ZKClient zkClient;
//
//    @Inject
//    ZKDiscoveryServiceProvider(ZKClient zkClient) {
//      this.zkClient = zkClient;
//    }
//
//    @Override
//    public ZKDiscoveryService get() {
//      return new ZKDiscoveryService(zkClient);
//    }
//  }
//
//  /**
//   * A DiscoveryServiceClient implementation that will namespace correctly for program service discovery.
//   * Otherwise it'll delegate to default one.
//   */
//  private static final class ProgramDiscoveryServiceClient implements DiscoveryServiceClient {
//
//    private static final Logger LOG = LoggerFactory.getLogger(ProgramDiscoveryServiceClient.class);
//    private static final long CACHE_EXPIRES_MINUTES = 1;
//
//    private final ZKClient zkClient;
//    private final ZKDiscoveryService masterDiscoveryService;
//    private final String twillNamespace;
//    private final LoadingCache<String, ZKDiscoveryService> clients;
//
//    @Inject
//    ProgramDiscoveryServiceClient(ZKClient zkClient,
//                                  CConfiguration configuration,
//                                  ZKDiscoveryService masterDiscoveryService) {
//      this.zkClient = zkClient;
//      this.masterDiscoveryService = masterDiscoveryService;
//      this.twillNamespace = configuration.get(Constants.CFG_TWILL_ZK_NAMESPACE);
//      this.clients = CacheBuilder.newBuilder()
//        .expireAfterAccess(CACHE_EXPIRES_MINUTES, TimeUnit.MINUTES)
//        .removalListener((RemovalListener<String, ZKDiscoveryService>) notification ->
//          Optional.ofNullable(notification.getValue()).ifPresent(ZKDiscoveryService::close))
//        .build(createClientLoader());
//    }
//
//    @Override
//    public ServiceDiscovered discover(final String name) {
//      for (ProgramType programType : ProgramType.values()) {
//        if (programType.isDiscoverable() && name.startsWith(programType.getDiscoverableTypeName() + ".")) {
//          return clients.getUnchecked(name).discover(name);
//        }
//      }
//      return masterDiscoveryService.discover(name);
//    }
//
//    private CacheLoader<String, ZKDiscoveryService> createClientLoader() {
//      return new CacheLoader<String, ZKDiscoveryService>() {
//        @Override
//        public ZKDiscoveryService load(String key) {
//          ProgramId programID = ServiceDiscoverable.getId(key);
//          String ns = String.format("%s/%s", twillNamespace, TwillAppNames.toTwillAppName(programID));
//          LOG.info("Create ZKDiscoveryClient for {}", ns);
//          return new ZKDiscoveryService(ZKClients.namespace(zkClient, ns));
//        }
//      };
//    }
//  }
}
