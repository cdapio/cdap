/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.metrics.MetricsReporterHook;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.internal.app.namespace.DefaultNamespaceEnsurer;
import co.cask.cdap.internal.app.runtime.artifact.SystemArtifactLoader;
import co.cask.cdap.internal.app.runtime.plugin.PluginService;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerService;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.route.store.RouteStore;
import co.cask.cdap.security.authorization.PrivilegesFetcherProxyService;
import co.cask.cdap.security.tools.KeyStores;
import co.cask.cdap.security.tools.SSLHandlerFactory;
import co.cask.http.HandlerHook;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * AppFabric Server.
 */
public class AppFabricServer extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricServer.class);

  private final DiscoveryService discoveryService;
  private final InetAddress hostname;
  private final SchedulerService schedulerService;
  private final ProgramRuntimeService programRuntimeService;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final NotificationService notificationService;
  private final Set<String> servicesNames;
  private final Set<String> handlerHookNames;
  private final StreamCoordinatorClient streamCoordinatorClient;
  private final ProgramLifecycleService programLifecycleService;
  private final SystemArtifactLoader systemArtifactLoader;
  private final PluginService pluginService;
  private final PrivilegesFetcherProxyService privilegesFetcherProxyService;
  private final AppVersionUpgradeService appVersionUpgradeService;
  private final RouteStore routeStore;
  private final CConfiguration cConf;
  private final SConfiguration sConf;
  private final boolean sslEnabled;

  private DefaultNamespaceEnsurer defaultNamespaceEnsurer;
  private SSLHandlerFactory sslHandlerFactory;
  private NettyHttpService httpService;
  private Set<HttpHandler> handlers;
  private MetricsCollectionService metricsCollectionService;

  /**
   * Construct the AppFabricServer with service factory and cConf coming from guice injection.
   */
  @Inject
  public AppFabricServer(CConfiguration cConf, SConfiguration sConf,
                         DiscoveryService discoveryService,
                         SchedulerService schedulerService, NotificationService notificationService,
                         @Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS) InetAddress hostname,
                         @Named(Constants.AppFabric.HANDLERS_BINDING) Set<HttpHandler> handlers,
                         @Nullable MetricsCollectionService metricsCollectionService,
                         ProgramRuntimeService programRuntimeService,
                         ApplicationLifecycleService applicationLifecycleService,
                         ProgramLifecycleService programLifecycleService,
                         StreamCoordinatorClient streamCoordinatorClient,
                         @Named("appfabric.services.names") Set<String> servicesNames,
                         @Named("appfabric.handler.hooks") Set<String> handlerHookNames,
                         NamespaceAdmin namespaceAdmin,
                         SystemArtifactLoader systemArtifactLoader,
                         PluginService pluginService,
                         PrivilegesFetcherProxyService privilegesFetcherProxyService,
                         AppVersionUpgradeService appVersionUpgradeService,
                         RouteStore routeStore) {
    this.hostname = hostname;
    this.discoveryService = discoveryService;
    this.schedulerService = schedulerService;
    this.handlers = handlers;
    this.cConf = cConf;
    this.sConf = sConf;
    this.metricsCollectionService = metricsCollectionService;
    this.programRuntimeService = programRuntimeService;
    this.notificationService = notificationService;
    this.servicesNames = servicesNames;
    this.handlerHookNames = handlerHookNames;
    this.applicationLifecycleService = applicationLifecycleService;
    this.streamCoordinatorClient = streamCoordinatorClient;
    this.programLifecycleService = programLifecycleService;
    this.systemArtifactLoader = systemArtifactLoader;
    this.pluginService = pluginService;
    this.privilegesFetcherProxyService = privilegesFetcherProxyService;
    this.appVersionUpgradeService = appVersionUpgradeService;
    this.routeStore = routeStore;
    this.defaultNamespaceEnsurer = new DefaultNamespaceEnsurer(namespaceAdmin);
    this.sslEnabled = cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED);
  }

  /**
   * Configures the AppFabricService pre-start.
   */
  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.APP_FABRIC_HTTP));
    Futures.allAsList(
      ImmutableList.of(
        notificationService.start(),
        schedulerService.start(),
        applicationLifecycleService.start(),
        systemArtifactLoader.start(),
        programRuntimeService.start(),
        streamCoordinatorClient.start(),
        programLifecycleService.start(),
        pluginService.start(),
        privilegesFetcherProxyService.start(),
        appVersionUpgradeService.start()
      )
    ).get();

    int serverPort;
    if (sslEnabled) {
      serverPort = cConf.getInt(Constants.AppFabric.SERVER_SSL_PORT);
      String password = generateRandomPassword();
      KeyStore ks = KeyStores.generatedCertKeyStore(sConf, password);

      this.sslHandlerFactory = new SSLHandlerFactory(ks, password);
    } else {
      serverPort = cConf.getInt(Constants.AppFabric.SERVER_PORT);
      this.sslHandlerFactory = null;
    }

    // Create handler hooks
    ImmutableList.Builder<HandlerHook> builder = ImmutableList.builder();
    for (String hook : handlerHookNames) {
      builder.add(new MetricsReporterHook(metricsCollectionService, hook));
    }

    // Run http service on random port
    NettyHttpService.Builder httpServiceBuilder = new CommonNettyHttpServiceBuilder(cConf,
                                                                                    Constants.Service.APP_FABRIC_HTTP)
      .setHost(hostname.getCanonicalHostName())
      .setPort(serverPort)
      .setHandlerHooks(builder.build())
      .addHttpHandlers(handlers)
      .setConnectionBacklog(cConf.getInt(Constants.AppFabric.BACKLOG_CONNECTIONS,
                                         Constants.AppFabric.DEFAULT_BACKLOG))
      .setExecThreadPoolSize(cConf.getInt(Constants.AppFabric.EXEC_THREADS,
                                          Constants.AppFabric.DEFAULT_EXEC_THREADS))
      .setBossThreadPoolSize(cConf.getInt(Constants.AppFabric.BOSS_THREADS,
                                          Constants.AppFabric.DEFAULT_BOSS_THREADS))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.AppFabric.WORKER_THREADS,
                                            Constants.AppFabric.DEFAULT_WORKER_THREADS));
    if (sslEnabled) {
      httpServiceBuilder.modifyChannelPipeline(new Function<ChannelPipeline, ChannelPipeline>() {
        @Override
        public ChannelPipeline apply(ChannelPipeline input) {
          LOG.debug("Adding ssl handler to the pipeline.");
          SslHandler sslHandler = sslHandlerFactory.create();
          // SSL handler needs to be the first handler in the pipeline.
          input.addFirst("ssl", sslHandler);
          return input;
        }
      });
    }

    httpService = httpServiceBuilder.build();

    // Add a listener so that when the service started, register with service discovery.
    // Remove from service discovery when it is stopped.
    httpService.addListener(new ServiceListenerAdapter() {

      private List<Cancellable> cancellables = Lists.newArrayList();

      @Override
      public void running() {
        String announceAddress = cConf.get(Constants.Service.MASTER_SERVICES_ANNOUNCE_ADDRESS,
                                           httpService.getBindAddress().getHostName());
        int announcePort = cConf.getInt(Constants.AppFabric.SERVER_ANNOUNCE_PORT,
                                        httpService.getBindAddress().getPort());

        final InetSocketAddress socketAddress = new InetSocketAddress(announceAddress, announcePort);
        LOG.info("AppFabric HTTP Service announced at {}", socketAddress);

        // Tag the discoverable's payload to mark it as supporting ssl.
        byte[] sslPayload = sslEnabled ? Constants.Security.SSL_URI_SCHEME.getBytes() : Bytes.EMPTY_BYTE_ARRAY;
        // TODO accept a list of services, and start them here
        // When it is running, register it with service discovery
        for (final String serviceName : servicesNames) {
          cancellables.add(discoveryService.register(ResolvingDiscoverable.of(
            new Discoverable(serviceName, socketAddress, sslPayload))));
        }
      }

      @Override
      public void terminated(State from) {
        LOG.info("AppFabric HTTP service stopped.");
        for (Cancellable cancellable : cancellables) {
          if (cancellable != null) {
            cancellable.cancel();
          }
        }
      }

      @Override
      public void failed(State from, Throwable failure) {
        LOG.info("AppFabric HTTP service stopped with failure.", failure);
        for (Cancellable cancellable : cancellables) {
          if (cancellable != null) {
            cancellable.cancel();
          }
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    httpService.startAndWait();
    defaultNamespaceEnsurer.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    routeStore.close();
    defaultNamespaceEnsurer.stopAndWait();
    httpService.stopAndWait();
    programRuntimeService.stopAndWait();
    schedulerService.stopAndWait();
    applicationLifecycleService.stopAndWait();
    systemArtifactLoader.stopAndWait();
    notificationService.stopAndWait();
    programLifecycleService.stopAndWait();
    pluginService.stopAndWait();
    privilegesFetcherProxyService.stopAndWait();
    appVersionUpgradeService.stopAndWait();
  }

  private static String generateRandomPassword() {
    // This works by choosing 130 bits from a cryptographically secure random bit generator, and encoding them in
    // base-32. 128 bits is considered to be cryptographically strong, but each digit in a base 32 number can encode
    // 5 bits, so 128 is rounded up to the next multiple of 5. Base 32 system uses alphabets A-Z and numbers 2-7
    return new BigInteger(130, new SecureRandom()).toString(32);
  }
}
