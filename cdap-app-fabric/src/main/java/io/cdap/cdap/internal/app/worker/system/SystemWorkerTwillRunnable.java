/*
 * Copyright © 2021-2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.system;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.DistributedArtifactManagerModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.KafkaClientModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.common.guice.ZkClientModule;
import io.cdap.cdap.common.guice.ZkDiscoveryModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.data.runtime.ConstantTransactionSystemClient;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.data2.metadata.writer.DefaultMetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.MessagingMetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import io.cdap.cdap.data2.transaction.TransactionSystemClientService;
import io.cdap.cdap.internal.app.namespace.LocalStorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.StorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactManager;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepositoryReaderWithLocalization;
import io.cdap.cdap.internal.app.runtime.distributed.remote.FireAndForgetTwillRunnerService;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteExecutionTwillRunnerService;
import io.cdap.cdap.internal.app.worker.RemoteWorkerPluginFinder;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerClient;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerService;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.guice.KafkaLogAppenderModule;
import io.cdap.cdap.logging.guice.RemoteLogAppenderModule;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.internal.messaging.MessagingClientModule;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.KeyManager;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.CoreSecurityModule;
import io.cdap.cdap.security.guice.FileBasedCoreSecurityModule;
import io.cdap.cdap.security.guice.SecureStoreClientModule;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link TwillRunnable} for running {@link SystemWorkerService}.
 */

public class SystemWorkerTwillRunnable extends AbstractTwillRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(SystemWorkerTwillRunnable.class);

  private SystemWorkerService systemWorker;
  private ArtifactLocalizerService artifactLocalizerService;
  private LogAppenderInitializer logAppenderInitializer;
  private MetricsCollectionService metricsCollectionService;

  public SystemWorkerTwillRunnable(String cConfFileName, String hConfFileName,
      String sConfFileName) {
    super(ImmutableMap.of("cConf", cConfFileName, "hConf", hConfFileName, "sConf", sConfFileName));
  }

  @VisibleForTesting
  static Injector createInjector(CConfiguration cConf, Configuration hConf, SConfiguration sConf) {
    List<Module> modules = new ArrayList<>();

    CoreSecurityModule coreSecurityModule = new FileBasedCoreSecurityModule() {
      @Override
      protected void bindKeyManager(Binder binder) {
        super.bindKeyManager(binder);
        expose(KeyManager.class);
      }
    };

    modules.add(new ConfigModule(cConf, hConf, sConf));
    modules.add(RemoteAuthenticatorModules.getDefaultModule());
    modules.add(new IOModule());
    modules.add(new AuthenticationContextModules().getMasterModule());
    modules.add(coreSecurityModule);
    modules.add(new MetricsClientRuntimeModule().getDistributedModules());

    modules.addAll(Arrays.asList(
        // Always use local table implementations, which use LevelDB.
        // In K8s, there won't be HBase and the cdap-site should be set to use SQL store for StructuredTable.
        new DataSetServiceModules().getStandaloneModules(),
        // The Dataset set modules are only needed to satisfy dependency injection
        new DataSetsModules().getStandaloneModules(),
        new MessagingClientModule(),
        new AuthorizationModule(),
        new AuthorizationEnforcementModule().getMasterModule(),
        Modules.override(new AppFabricServiceRuntimeModule(cConf).getDistributedModules())
            .with(new AbstractModule() {
              // To enable localisation of artifacts
              @Override
              protected void configure() {
                bind(StorageProviderNamespaceAdmin.class).to(
                    LocalStorageProviderNamespaceAdmin.class);
              }
            }, getArtifactManagerModules(cConf)),
        Modules.override(new ProgramRunnerRuntimeModule().getDistributedModules(true))
            .with(new AbstractModule() {
              @Override
              protected void configure() {
                bind(RemoteExecutionTwillRunnerService.class)
                    .to(FireAndForgetTwillRunnerService.class)
                    .in(Scopes.SINGLETON);
              }
            }),
        new SecureStoreClientModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            install(new StorageModule());
            install(new TransactionExecutorModule());
            bind(TransactionSystemClientService.class).to(
                DelegatingTransactionSystemClientService.class);
            bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class);
          }
        },
        new DFSLocationModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetadataPublisher.class).to(MessagingMetadataPublisher.class);
            bind(MetadataServiceClient.class).to(DefaultMetadataServiceClient.class);
          }
        }
    ));

    // If MasterEnvironment is not available, assuming it is the old hadoop stack with ZK, Kafka
    MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();

    if (masterEnv == null) {
      modules.add(new ZkClientModule());
      modules.add(new ZkDiscoveryModule());
      modules.add(new KafkaClientModule());
      modules.add(new KafkaLogAppenderModule());
    } else {
      modules.add(new AbstractModule() {
        @Override
        protected void configure() {
          bind(DiscoveryService.class)
              .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
          bind(DiscoveryServiceClient.class)
              .toProvider(
                  new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));
        }
      });
      modules.add(new RemoteLogAppenderModule());
      modules.add(new AbstractModule() {
        @Override
        protected void configure() {
          bind(TwillRunnerService.class).toProvider(
              new SupplierProviderBridge<>(masterEnv.getTwillRunnerSupplier()))
              .in(Scopes.SINGLETON);
          bind(TwillRunner.class).to(TwillRunnerService.class);
        }
      });

      if (coreSecurityModule.requiresZKClient()) {
        modules.add(new ZkClientModule());
      }
    }

    return Guice.createInjector(modules);
  }

  private static Module getArtifactManagerModules(CConfiguration cConf) {
    if (cConf.getBoolean(Constants.SystemWorker.ARTIFACT_LOCALIZER_ENABLED)) {
      return new AbstractModule() {
        @Override
        protected void configure() {
          bind(PluginFinder.class).to(RemoteWorkerPluginFinder.class);
          bind(ArtifactRepositoryReader.class).to(
              RemoteArtifactRepositoryReaderWithLocalization.class)
              .in(Scopes.SINGLETON);
          bind(ArtifactLocalizerClient.class).in(Scopes.SINGLETON);
          OptionalBinder.newOptionalBinder(binder(), ArtifactLocalizerClient.class);
          install(new FactoryModuleBuilder().implement(ArtifactManager.class,
              RemoteArtifactManager.class)
              .build(ArtifactManagerFactory.class));
        }
      };
    } else {
      return new DistributedArtifactManagerModule();
    }
  }

  @Override
  public void initialize(TwillContext context) {
    super.initialize(context);

    try {
      doInitialize();
    } catch (Exception e) {
      LOG.error("Encountered error while initializing SystemWorkerTwillRunnable", e);
      Throwables.propagateIfPossible(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    CompletableFuture<Service.State> future = new CompletableFuture<>();
    systemWorker.addListener(new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        future.complete(from);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        future.completeExceptionally(failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    LOG.debug("Starting system worker");
    systemWorker.start();
    if (artifactLocalizerService != null) {
      artifactLocalizerService.start();
    }

    try {
      Uninterruptibles.getUninterruptibly(future);
      LOG.debug("System worker stopped");
    } catch (ExecutionException e) {
      LOG.warn("System worker stopped with exception", e);
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping system worker");
    Optional.ofNullable(metricsCollectionService).map(MetricsCollectionService::stop);
    if (systemWorker != null) {
      systemWorker.stop();
    }
    if (artifactLocalizerService != null) {
      artifactLocalizerService.stop();
    }
  }

  @Override
  public void destroy() {
    if (logAppenderInitializer != null) {
      logAppenderInitializer.close();
    }
  }

  @VisibleForTesting
  void doInitialize() throws Exception {
    CConfiguration cConf = CConfiguration.create(new File(getArgument("cConf")).toURI().toURL());

    // Overwrite the app fabric temp directory with the task worker temp directory
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, cConf.get(Constants.SystemWorker.LOCAL_DATA_DIR));

    // We replace TWILL_CONTROLLER_START_SECONDS value with the one specific for system worker
    cConf.set(Constants.AppFabric.TWILL_CONTROLLER_START_SECONDS,
        cConf.get(Constants.SystemWorker.TWILL_CONTROLLER_START_SECONDS));

    Configuration hConf = new Configuration();
    hConf.clear();
    hConf.addResource(new File(getArgument("hConf")).toURI().toURL());

    SConfiguration sConf = SConfiguration.create(new File(getArgument("sConf")));

    Injector injector = createInjector(cConf, hConf, sConf);

    // Initialize logging context
    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    logAppenderInitializer.initialize();

    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsCollectionService.startAndWait();

    LoggingContext loggingContext = new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
        Constants.Logging.COMPONENT_NAME,
        SystemWorkerTwillApplication.NAME);
    LoggingContextAccessor.setLoggingContext(loggingContext);
    systemWorker = injector.getInstance(SystemWorkerService.class);
    if (cConf.getBoolean(Constants.SystemWorker.ARTIFACT_LOCALIZER_ENABLED)) {
      artifactLocalizerService = injector.getInstance(ArtifactLocalizerService.class);
    }
  }
}
