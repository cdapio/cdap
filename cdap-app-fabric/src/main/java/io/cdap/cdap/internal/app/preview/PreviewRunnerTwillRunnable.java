/*
 * Copyright © 2020-2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Binding;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.app.preview.PreviewConfigModule;
import io.cdap.cdap.app.preview.PreviewRunner;
import io.cdap.cdap.app.preview.PreviewRunnerManager;
import io.cdap.cdap.app.preview.PreviewRunnerManagerModule;
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
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactory;
import io.cdap.cdap.internal.app.deploy.InMemoryConfigurator;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.k8s.PreviewRequestPollerInfo;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerClient;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.guice.KafkaLogAppenderModule;
import io.cdap.cdap.logging.guice.RemoteLogAppenderModule;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.twill.ExtendedTwillContext;
import io.cdap.cdap.messaging.guice.MessagingServiceModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.SecureStoreClientModule;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.spi.data.StorageProvider;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link TwillRunnable} for running {@link PreviewRunner}.
 */
public class PreviewRunnerTwillRunnable extends AbstractTwillRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(PreviewRunnerTwillRunnable.class);

  private PreviewRunnerManager previewRunnerManager;
  private LogAppenderInitializer logAppenderInitializer;
  private StorageProvider storageProvider;

  public PreviewRunnerTwillRunnable(String cConfFileName, String hConfFileName) {
    super(ImmutableMap.of("cConf", cConfFileName, "hConf", hConfFileName));
  }

  @Override
  public void initialize(TwillContext context) {
    super.initialize(context);

    try {
      doInitialize(context);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    CompletableFuture<Service.State> future = new CompletableFuture<>();
    previewRunnerManager.addListener(new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        future.complete(from);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        future.completeExceptionally(failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    LOG.debug("Starting preview runner manager");
    previewRunnerManager.start();

    try {
      Uninterruptibles.getUninterruptibly(future);
      LOG.debug("Preview runner manager stopped");
    } catch (ExecutionException e) {
      LOG.warn("Preview runner manager stopped with exception", e);
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping preview runner manager");
    previewRunnerManager.stop();
  }

  @Override
  public void destroy() {
    if (storageProvider != null) {
      try {
        storageProvider.close();
      } catch (Exception e) {
        LOG.warn("Exception raised when closing storage provider", e);
      }
    }
    logAppenderInitializer.close();
  }

  private void doInitialize(TwillContext context) throws Exception {
    Configuration hConf = new Configuration();
    hConf.clear();
    hConf.addResource(new File(getArgument("hConf")).toURI().toURL());

    PreviewRequestPollerInfo pollerInfo;
    if (context instanceof ExtendedTwillContext) {
      pollerInfo = new PreviewRequestPollerInfo(context.getInstanceId(),
          ((ExtendedTwillContext) context).getUID());
    } else {
      pollerInfo = new PreviewRequestPollerInfo(context.getInstanceId(), null);
    }

    CConfiguration cConf = CConfiguration.create(new File(getArgument("cConf")).toURI().toURL());

    LOG.debug("Initializing preview runner with poller info {} in total {} runners",
        pollerInfo, context.getInstanceCount());

    Injector injector = createInjector(cConf, hConf, pollerInfo);

    // Initialize logging context
    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    logAppenderInitializer.initialize();

    LoggingContext loggingContext = new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
        Constants.Logging.COMPONENT_NAME,
        PreviewRunnerTwillApplication.NAME);
    LoggingContextAccessor.setLoggingContext(loggingContext);

    // Optionally get the storage provider. It is for destroy() method to close it on shutdown.
    Binding<StorageProvider> storageBinding = injector.getExistingBinding(
        Key.get(StorageProvider.class));
    if (storageBinding != null) {
      storageProvider = storageBinding.getProvider().get();
    }

    previewRunnerManager = injector.getInstance(PreviewRunnerManager.class);
  }

  @VisibleForTesting
  static Injector createInjector(CConfiguration cConf, Configuration hConf,
      PreviewRequestPollerInfo pollerInfo) {
    List<Module> modules = new ArrayList<>();

    SConfiguration sConf = SConfiguration.create();

    modules.add(new ConfigModule(cConf, hConf, sConf));
    modules.add(RemoteAuthenticatorModules.getDefaultModule());
    modules.add(new PreviewConfigModule(cConf, hConf, sConf));
    modules.add(new IOModule());

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
    }

    modules.add(new PreviewRunnerManagerModule().getDistributedModules());
    modules.add(new MessagingServiceModule(cConf));
    modules.add(new SecureStoreClientModule());
    // Needed for InMemoryProgramRunnerModule. We use local metadata reader/publisher to avoid conflicting with
    // metadata stored in AppFabric.
    modules.add(new DFSLocationModule());
    // Configurator tasks should be executed in-memory since it is in the preview runner pod.
    modules.add(new FactoryModuleBuilder().implement(Configurator.class, InMemoryConfigurator.class)
        .build(ConfiguratorFactory.class));

    modules.add(new AuthenticationContextModules().getMasterWorkerModule());
    modules.add(new AuthorizationEnforcementModule().getNoOpModules());

    byte[] pollerInfoBytes = Bytes.toBytes(new Gson().toJson(pollerInfo));
    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class);

        bind(PreviewRequestPollerInfoProvider.class).toInstance(() -> pollerInfoBytes);

        // Artifact Repository should use RemoteArtifactRepository.
        // TODO(CDAP-19041): Consider adding a remote artifact respository handler to the preview manager so that
        //  preview runners do not have to talk directly to app-fabric for artifacts to prevent hot-spotting.
        bind(ArtifactRepositoryReader.class).to(RemoteArtifactRepositoryReader.class)
            .in(Scopes.SINGLETON);
        bind(ArtifactRepository.class).to(RemoteArtifactRepository.class);
        // Use artifact localizer client for preview.
        bind(PluginFinder.class).to(PreviewPluginFinder.class);
        bind(ArtifactLocalizerClient.class).in(Scopes.SINGLETON);
        // Preview runner pods should not have any elevated privileges, so use the current UGI.
        bind(UGIProvider.class).to(CurrentUGIProvider.class);
      }
    });

    return Guice.createInjector(modules);
  }
}
