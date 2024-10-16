/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.auditlogging.AuditLogPublisherService;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.MonitorHandlerModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.store.ServiceStore;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.SystemWorker;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.service.RetryOnStartFailureService;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.audit.AuditModule;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.metadata.writer.DefaultMetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.MessagingMetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.internal.app.namespace.LocalStorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.StorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.services.AppFabricServer;
import io.cdap.cdap.internal.app.worker.TaskWorkerServiceLauncher;
import io.cdap.cdap.internal.app.worker.system.SystemWorkerServiceLauncher;
import io.cdap.cdap.internal.events.EventPublishManager;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.guice.MessagingServiceModule;
import io.cdap.cdap.metrics.guice.MetricsStoreModule;
import io.cdap.cdap.operations.OperationalStatsService;
import io.cdap.cdap.operations.guice.OperationalStatsModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.service.DefaultAuditLogPublisherService;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
import io.cdap.cdap.security.store.SecureStoreService;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.zookeeper.ZKClientService;

/**
 * The main class to run app-fabric and other supporting services.
 */
public class AppFabricServiceMain extends AbstractServiceMain<EnvironmentOptions> {

  /**
   * Main entry point.
   */
  public static void main(String[] args) throws Exception {
    main(AppFabricServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv,
      EnvironmentOptions options, CConfiguration cConf) {
    return Arrays.asList(
        // Always use local table implementations, which use LevelDB.
        // In K8s, there won't be HBase and the cdap-site should be set to use SQL store for StructuredTable.
        new DataSetServiceModules().getStandaloneModules(),
        // The Dataset set modules are only needed to satisfy dependency injection
        new DataSetsModules().getStandaloneModules(),
        new MetricsStoreModule(),
        new MessagingServiceModule(cConf),
        new AuditModule(),
        new AuthorizationModule(),
        new AuthorizationEnforcementModule().getMasterModule(),
        Modules.override(new AppFabricServiceRuntimeModule(cConf).getDistributedModules())
            .with(new AbstractModule() {
              @Override
              protected void configure() {
                bind(StorageProviderNamespaceAdmin.class).to(
                    LocalStorageProviderNamespaceAdmin.class);
              }
            }),
        new ProgramRunnerRuntimeModule().getDistributedModules(true),
        new MonitorHandlerModule(false),
        new SecureStoreServerModule(),
        new OperationalStatsModule(),
        getDataFabricModule(),
        new DFSLocationModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(TwillRunnerService.class).toProvider(
                new SupplierProviderBridge<>(masterEnv.getTwillRunnerSupplier()))
                .in(Scopes.SINGLETON);
            bind(TwillRunner.class).to(TwillRunnerService.class);

            // TODO (CDAP-14677): find a better way to inject metadata publisher
            bind(MetadataPublisher.class).to(MessagingMetadataPublisher.class);
            bind(MetadataServiceClient.class).to(DefaultMetadataServiceClient.class);
            bind(AuditLogPublisherService.class).to(DefaultAuditLogPublisherService.class);
          }
        }
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
      List<? super AutoCloseable> closeableResources,
      MasterEnvironment masterEnv, MasterEnvironmentContext masterEnvContext,
      EnvironmentOptions options) {
    final CConfiguration cConf = injector.getInstance(CConfiguration.class);
    closeableResources.add(injector.getInstance(AccessControllerInstantiator.class));
    services.add(injector.getInstance(OperationalStatsService.class));
    services.add(injector.getInstance(SecureStoreService.class));
    services.add(injector.getInstance(DatasetOpExecutorService.class));
    services.add(injector.getInstance(ServiceStore.class));

    Binding<ZKClientService> zkBinding = injector.getExistingBinding(
        Key.get(ZKClientService.class));
    if (zkBinding != null) {
      services.add(zkBinding.getProvider().get());
    }

    // Start both the remote TwillRunnerService and regular TwillRunnerService
    TwillRunnerService remoteTwillRunner = injector.getInstance(Key.get(TwillRunnerService.class,
        Constants.AppFabric.RemoteExecution.class));
    services.add(new TwillRunnerServiceWrapper(remoteTwillRunner));
    services.add(new TwillRunnerServiceWrapper(injector.getInstance(TwillRunnerService.class)));
    services.add(new RetryOnStartFailureService(() -> injector.getInstance(DatasetService.class),
        RetryStrategies.exponentialDelay(200, 5000, TimeUnit.MILLISECONDS)));
    services.add(injector.getInstance(AppFabricServer.class));
    services.add(new RetryOnStartFailureService(
        () -> injector.getInstance(NamespaceInitializerService.class),
        RetryStrategies.exponentialDelay(200, 5000, TimeUnit.MILLISECONDS)));

    services.add(injector.getInstance(AuditLogPublisherService.class));

    if (cConf.getBoolean(Constants.TaskWorker.POOL_ENABLE)) {
      services.add(injector.getInstance(TaskWorkerServiceLauncher.class));
    }

    if (cConf.getBoolean(SystemWorker.POOL_ENABLE)) {
      services.add(injector.getInstance(SystemWorkerServiceLauncher.class));
    }

    // Event publisher could rely on task workers for token generated for security enabled deployments
    services.add(injector.getInstance(EventPublishManager.class));

    // Adds the master environment tasks
    masterEnv.getTasks()
        .forEach(task -> services.add(new MasterTaskExecutorService(task, masterEnvContext)));
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext(EnvironmentOptions options) {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
        Constants.Logging.COMPONENT_NAME,
        Constants.Service.APP_FABRIC_HTTP);
  }

}
