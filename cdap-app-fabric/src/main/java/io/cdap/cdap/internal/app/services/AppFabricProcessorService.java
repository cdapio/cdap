/*
 * Copyright © 2025 Cask Data, Inc.
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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service.State;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.AppFabric;
import io.cdap.cdap.common.conf.Constants.Service;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.bootstrap.BootstrapService;
import io.cdap.cdap.internal.operation.OperationNotificationSubscriberService;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.internal.sysapp.SystemAppManagementService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.scheduler.CoreSchedulerService;
import io.cdap.cdap.security.auth.AuditLogSubscriberService;
import io.cdap.http.HttpHandler;
import io.cdap.http.NettyHttpService;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AppFabric Processor Service which runs messaging subscriber services.
 */
public class AppFabricProcessorService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricProcessorService.class);

  private final DiscoveryService discoveryService;
  private final InetAddress hostname;
  private final ProgramRuntimeService programRuntimeService;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final Set<String> servicesNames;
  private final ProgramNotificationSubscriberService programNotificationSubscriberService;
  private final ProgramStopSubscriberService programStopSubscriberService;
  private final AuditLogSubscriberService auditLogSubscriberService;
  private final RunRecordCorrectorService runRecordCorrectorService;
  private final RunDataTimeToLiveService runDataTimeToLiveService;
  private final ProgramRunStatusMonitorService programRunStatusMonitorService;
  private final FlowControlService runRecordCounterService;
  private final CoreSchedulerService coreSchedulerService;
  private final ProvisioningService provisioningService;
  private final BootstrapService bootstrapService;
  private final SystemAppManagementService systemAppManagementService;
  private final OperationNotificationSubscriberService operationNotificationSubscriberService;
  private final CConfiguration cConf;
  private final SConfiguration sConf;
  private final boolean sslEnabled;
  private CommonNettyHttpServiceFactory commonNettyHttpServiceFactory;
  private Cancellable cancelHttpService;
  private Set<HttpHandler> handlers;

  /**
   * Construct the AppFabricProcessorService with service factory and cConf coming from guice injection.
   */
  @Inject
  public AppFabricProcessorService(CConfiguration cConf,
      SConfiguration sConf,
      DiscoveryService discoveryService,
      @Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS) InetAddress hostname,
      @Named(Constants.AppFabric.PROCESSOR_HANDLERS_BINDING) Set<HttpHandler> handlers,
      ProgramRuntimeService programRuntimeService,
      RunRecordCorrectorService runRecordCorrectorService,
      ProgramRunStatusMonitorService programRunStatusMonitorService,
      ApplicationLifecycleService applicationLifecycleService,
      @Named("appfabric.processor.services.names") Set<String> servicesNames,
      CommonNettyHttpServiceFactory commonNettyHttpServiceFactory,
      ProgramNotificationSubscriberService programNotificationSubscriberService,
      ProgramStopSubscriberService programStopSubscriberService,
      AuditLogSubscriberService auditLogSubscriberService,
      CoreSchedulerService coreSchedulerService,
      ProvisioningService provisioningService,
      BootstrapService bootstrapService,
      SystemAppManagementService systemAppManagementService,
      FlowControlService runRecordCounterService,
      RunDataTimeToLiveService runDataTimeToLiveService,
      OperationNotificationSubscriberService operationNotificationSubscriberService) {
    this.hostname = hostname;
    this.discoveryService = discoveryService;
    this.handlers = handlers;
    this.cConf = cConf;
    this.sConf = sConf;
    this.sslEnabled = cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED);
    this.servicesNames = servicesNames;
    this.commonNettyHttpServiceFactory = commonNettyHttpServiceFactory;
    this.programRuntimeService = programRuntimeService;
    this.applicationLifecycleService = applicationLifecycleService;
    this.programNotificationSubscriberService = programNotificationSubscriberService;
    this.programStopSubscriberService = programStopSubscriberService;
    this.auditLogSubscriberService = auditLogSubscriberService;
    this.runRecordCorrectorService = runRecordCorrectorService;
    this.programRunStatusMonitorService = programRunStatusMonitorService;
    this.coreSchedulerService = coreSchedulerService;
    this.provisioningService = provisioningService;
    this.bootstrapService = bootstrapService;
    this.systemAppManagementService = systemAppManagementService;
    this.runRecordCounterService = runRecordCounterService;
    this.runDataTimeToLiveService = runDataTimeToLiveService;
    this.operationNotificationSubscriberService = operationNotificationSubscriberService;
  }

  /**
   * Configures the AppFabricProcessorService pre-start.
   */
  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(
        new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
            Constants.Logging.COMPONENT_NAME,
            Service.APP_FABRIC_PROCESSOR));
    LOG.info("Starting AppFabric processor service.");
    FeatureFlagsProvider featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
    // Only for RBAC instances
    if (Feature.DATAPLANE_AUDIT_LOGGING.isEnabled(featureFlagsProvider)
        && cConf.getBoolean(Constants.Security.ENABLED)) {
      if (auditLogSubscriberService.state() == State.NEW) {
        auditLogSubscriberService.startAsync();
      }
    }

    if (provisioningService.state() == State.NEW) {
      provisioningService.startAsync();
    }
    if (applicationLifecycleService.state() == State.NEW) {
      applicationLifecycleService.startAsync();
    }
    if (bootstrapService.state() == State.NEW) {
      bootstrapService.startAsync();
    }
    if (programRuntimeService.state() == State.NEW) {
      programRuntimeService.startAsync();
    }
    if (programNotificationSubscriberService.state() == State.NEW) {
      programNotificationSubscriberService.startAsync();
    }
    if (programStopSubscriberService.state() == State.NEW) {
      programStopSubscriberService.startAsync();
    }
    if (runRecordCorrectorService.state() == State.NEW) {
      runRecordCorrectorService.startAsync();
    }
    if (programRunStatusMonitorService.state() == State.NEW) {
      programRunStatusMonitorService.startAsync();
    }
    if (coreSchedulerService.state() == State.NEW) {
      coreSchedulerService.startAsync();
    }
    if (runRecordCounterService.state() == State.NEW) {
      runRecordCounterService.startAsync();
    }
    if (runDataTimeToLiveService.state() == State.NEW) {
      runDataTimeToLiveService.startAsync();
    }
    if (operationNotificationSubscriberService.state() == State.NEW) {
      operationNotificationSubscriberService.startAsync();
    }

    if (Feature.DATAPLANE_AUDIT_LOGGING.isEnabled(featureFlagsProvider)
        && cConf.getBoolean(Constants.Security.ENABLED)) {
      auditLogSubscriberService.awaitRunning();
    }
    provisioningService.awaitRunning();
    applicationLifecycleService.awaitRunning();
    bootstrapService.awaitRunning();
    programRuntimeService.awaitRunning();
    programNotificationSubscriberService.awaitRunning();
    programStopSubscriberService.awaitRunning();
    runRecordCorrectorService.awaitRunning();
    programRunStatusMonitorService.awaitRunning();
    coreSchedulerService.awaitRunning();
    runRecordCounterService.awaitRunning();
    runDataTimeToLiveService.awaitRunning();
    operationNotificationSubscriberService.awaitRunning();

    // Run http service on random port
    NettyHttpService.Builder httpServiceBuilder = commonNettyHttpServiceFactory
        .builder(Constants.Service.APP_FABRIC_HTTP)
        .setHost(hostname.getCanonicalHostName())
        .setHttpHandlers(handlers)
        .setConnectionBacklog(cConf.getInt(Constants.AppFabric.BACKLOG_CONNECTIONS,
            Constants.AppFabric.DEFAULT_BACKLOG))
        .setExecThreadPoolSize(cConf.getInt(Constants.AppFabric.EXEC_THREADS,
            Constants.AppFabric.DEFAULT_EXEC_THREADS))
        .setBossThreadPoolSize(cConf.getInt(Constants.AppFabric.BOSS_THREADS,
            Constants.AppFabric.DEFAULT_BOSS_THREADS))
        .setWorkerThreadPoolSize(cConf.getInt(Constants.AppFabric.WORKER_THREADS,
            Constants.AppFabric.DEFAULT_WORKER_THREADS))
        .setPort(cConf.getInt(Constants.AppFabric.PROCESSOR_PORT));
    if (sslEnabled) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(httpServiceBuilder);
    }

    cancelHttpService = startHttpService(httpServiceBuilder.build());
    LOG.info("AppFabric processor service started.");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping AppFabric processor service.");
    cancelHttpService.cancel();
    if (coreSchedulerService.isRunning()) {
      coreSchedulerService.stopAsync().awaitTerminated();
    }
    if (bootstrapService.isRunning()) {
      bootstrapService.stopAsync().awaitTerminated();
    }
    if (systemAppManagementService.isRunning()) {
      systemAppManagementService.stopAsync().awaitTerminated();
    }
    if (programRuntimeService.isRunning()) {
      programRuntimeService.stopAsync().awaitTerminated();
    }
    if (applicationLifecycleService.isRunning()) {
      applicationLifecycleService.stopAsync().awaitTerminated();
    }
    if (programNotificationSubscriberService.isRunning()) {
      programNotificationSubscriberService.stopAsync().awaitTerminated();
    }
    if (programStopSubscriberService.isRunning()) {
      programStopSubscriberService.stopAsync().awaitTerminated();
    }
    if (runRecordCorrectorService.isRunning()) {
      runRecordCorrectorService.stopAsync().awaitTerminated();
    }
    if (programRunStatusMonitorService.isRunning()) {
      programRunStatusMonitorService.stopAsync().awaitTerminated();
    }
    if (provisioningService.isRunning()) {
      provisioningService.stopAsync().awaitTerminated();
    }
    if (runRecordCounterService.isRunning()) {
      runRecordCounterService.stopAsync().awaitTerminated();
    }
    if (runDataTimeToLiveService.isRunning()) {
      runDataTimeToLiveService.stopAsync().awaitTerminated();
    }
    if (operationNotificationSubscriberService.isRunning()) {
      operationNotificationSubscriberService.stopAsync().awaitTerminated();
    }
    if (auditLogSubscriberService.isRunning()) {
      auditLogSubscriberService.stopAsync().awaitTerminated();
    }
    LOG.info("AppFabric processor service stopped.");
  }

  private Cancellable startHttpService(NettyHttpService httpService) throws Exception {
    httpService.start();

    String announceAddress = cConf.get(Constants.Service.MASTER_SERVICES_ANNOUNCE_ADDRESS,
        httpService.getBindAddress().getHostName());
    int announcePort = cConf.getInt(AppFabric.PROCESSOR_ANNOUNCE_PORT,
        httpService.getBindAddress().getPort());

    final InetSocketAddress socketAddress = new InetSocketAddress(announceAddress, announcePort);
    LOG.info("AppFabric Processor HTTP Service announced at {}", socketAddress);

    // Tag the discoverable's payload to mark it as supporting ssl.
    URIScheme uriScheme = sslEnabled ? URIScheme.HTTPS : URIScheme.HTTP;

    final List<Cancellable> cancellables = new ArrayList<>();
    for (final String serviceName : servicesNames) {
      cancellables.add(discoveryService.register(
          ResolvingDiscoverable.of(uriScheme.createDiscoverable(serviceName, socketAddress))));
    }

    return () -> {
      LOG.debug("Stopping AppFabric Processor HTTP service.");
      for (Cancellable cancellable : cancellables) {
        if (cancellable != null) {
          cancellable.cancel();
        }
      }

      try {
        httpService.stop();
      } catch (Exception e) {
        LOG.warn("Exception raised when stopping AppFabric Processor service", e);
      }

      LOG.info("AppFabric Processor HTTP service stopped.");
    };
  }
}
