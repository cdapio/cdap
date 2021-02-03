/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.metrics.MetricsReporterHook;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.bootstrap.BootstrapService;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.internal.sysapp.SystemAppManagementService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.scheduler.CoreSchedulerService;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.spi.data.transaction.TxCallable;
import io.cdap.cdap.store.DefaultNamespaceStore;
import io.cdap.http.HttpHandler;
import io.cdap.http.NettyHttpService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * AppFabric Server.
 */
public class AppFabricServer extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricServer.class);

  private final DiscoveryService discoveryService;
  private final InetAddress hostname;
  private final ProgramRuntimeService programRuntimeService;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final Set<String> servicesNames;
  private final Set<String> handlerHookNames;
  private final ProgramNotificationSubscriberService programNotificationSubscriberService;
  private final RunRecordCorrectorService runRecordCorrectorService;
  private final CoreSchedulerService coreSchedulerService;
  private final ProvisioningService provisioningService;
  private final BootstrapService bootstrapService;
  private final SystemAppManagementService systemAppManagementService;
  private final CConfiguration cConf;
  private final SConfiguration sConf;
  private final boolean sslEnabled;
  private final TransactionRunner transactionRunner;

  private Cancellable cancelHttpService;
  private Set<HttpHandler> handlers;
  private MetricsCollectionService metricsCollectionService;

  /**
   * Construct the AppFabricServer with service factory and cConf coming from guice injection.
   */
  @Inject
  public AppFabricServer(CConfiguration cConf, SConfiguration sConf,
                         DiscoveryService discoveryService,
                         @Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS) InetAddress hostname,
                         @Named(Constants.AppFabric.HANDLERS_BINDING) Set<HttpHandler> handlers,
                         @Nullable MetricsCollectionService metricsCollectionService,
                         ProgramRuntimeService programRuntimeService,
                         RunRecordCorrectorService runRecordCorrectorService,
                         ApplicationLifecycleService applicationLifecycleService,
                         ProgramNotificationSubscriberService programNotificationSubscriberService,
                         @Named("appfabric.services.names") Set<String> servicesNames,
                         @Named("appfabric.handler.hooks") Set<String> handlerHookNames,
                         CoreSchedulerService coreSchedulerService,
                         ProvisioningService provisioningService,
                         BootstrapService bootstrapService,
                         SystemAppManagementService systemAppManagementService,
                         TransactionRunner transactionRunner) {
    this.hostname = hostname;
    this.discoveryService = discoveryService;
    this.handlers = handlers;
    this.cConf = cConf;
    this.sConf = sConf;
    this.metricsCollectionService = metricsCollectionService;
    this.programRuntimeService = programRuntimeService;
    this.servicesNames = servicesNames;
    this.handlerHookNames = handlerHookNames;
    this.applicationLifecycleService = applicationLifecycleService;
    this.programNotificationSubscriberService = programNotificationSubscriberService;
    this.runRecordCorrectorService = runRecordCorrectorService;
    this.sslEnabled = cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED);
    this.coreSchedulerService = coreSchedulerService;
    this.provisioningService = provisioningService;
    this.bootstrapService = bootstrapService;
    this.systemAppManagementService = systemAppManagementService;
    this.transactionRunner = transactionRunner;
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
        provisioningService.start(),
        applicationLifecycleService.start(),
        bootstrapService.start(),
        programRuntimeService.start(),
        programNotificationSubscriberService.start(),
        runRecordCorrectorService.start(),
        coreSchedulerService.start()
      )
    ).get();

    // Create handler hooks
    List<MetricsReporterHook> handlerHooks = handlerHookNames.stream()
      .map(name -> new MetricsReporterHook(metricsCollectionService, name))
      .collect(Collectors.toList());

    // Run http service on random port
    NettyHttpService.Builder httpServiceBuilder = new CommonNettyHttpServiceBuilder(cConf,
                                                                                    Constants.Service.APP_FABRIC_HTTP)
      .setHost(hostname.getCanonicalHostName())
      .setHandlerHooks(handlerHooks)
      .setHttpHandlers(handlers)
      .setConnectionBacklog(cConf.getInt(Constants.AppFabric.BACKLOG_CONNECTIONS,
                                         Constants.AppFabric.DEFAULT_BACKLOG))
      .setExecThreadPoolSize(cConf.getInt(Constants.AppFabric.EXEC_THREADS,
                                          Constants.AppFabric.DEFAULT_EXEC_THREADS))
      .setBossThreadPoolSize(cConf.getInt(Constants.AppFabric.BOSS_THREADS,
                                          Constants.AppFabric.DEFAULT_BOSS_THREADS))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.AppFabric.WORKER_THREADS,
                                            Constants.AppFabric.DEFAULT_WORKER_THREADS))
      .setPort(cConf.getInt(Constants.AppFabric.SERVER_PORT));
    if (sslEnabled) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(httpServiceBuilder);
    }

    cancelHttpService = startHttpService(httpServiceBuilder.build());
    long applicationCount = TransactionRunners.run(transactionRunner,
                                                   (TxCallable<Long>) context ->
                                                     AppMetadataStore.create(context).getApplicationCount());
    long namespaceCount = new DefaultNamespaceStore(transactionRunner).getNamespaceCount();

    metricsCollectionService.getContext(Collections.emptyMap()).gauge(Constants.Metrics.Program.APPLICATION_COUNT,
                                                                      applicationCount);
    metricsCollectionService.getContext(Collections.emptyMap()).gauge(Constants.Metrics.Program.NAMESPACE_COUNT,
                                                                      namespaceCount);
  }

  @Override
  protected void shutDown() throws Exception {
    coreSchedulerService.stopAndWait();
    bootstrapService.stopAndWait();
    systemAppManagementService.stopAndWait();
    cancelHttpService.cancel();
    programRuntimeService.stopAndWait();
    applicationLifecycleService.stopAndWait();
    programNotificationSubscriberService.stopAndWait();
    runRecordCorrectorService.stopAndWait();
    provisioningService.stopAndWait();
  }

  private Cancellable startHttpService(NettyHttpService httpService) throws Exception {
    httpService.start();

    String announceAddress = cConf.get(Constants.Service.MASTER_SERVICES_ANNOUNCE_ADDRESS,
                                       httpService.getBindAddress().getHostName());
    int announcePort = cConf.getInt(Constants.AppFabric.SERVER_ANNOUNCE_PORT,
                                    httpService.getBindAddress().getPort());

    final InetSocketAddress socketAddress = new InetSocketAddress(announceAddress, announcePort);
    LOG.info("AppFabric HTTP Service announced at {}", socketAddress);

    // Tag the discoverable's payload to mark it as supporting ssl.
    URIScheme uriScheme = sslEnabled ? URIScheme.HTTPS : URIScheme.HTTP;
    // TODO accept a list of services, and start them here
    // When it is running, register it with service discovery

    final List<Cancellable> cancellables = new ArrayList<>();
    for (final String serviceName : servicesNames) {
      cancellables.add(discoveryService.register(
        ResolvingDiscoverable.of(uriScheme.createDiscoverable(serviceName, socketAddress))));
    }

    return () -> {
      LOG.debug("Stopping AppFabric HTTP service.");
      for (Cancellable cancellable : cancellables) {
        if (cancellable != null) {
          cancellable.cancel();
        }
      }

      try {
        httpService.stop();
      } catch (Exception e) {
        LOG.warn("Exception raised when stopping AppFabric HTTP service", e);
      }

      LOG.info("AppFabric HTTP service stopped.");
    };
  }
}
