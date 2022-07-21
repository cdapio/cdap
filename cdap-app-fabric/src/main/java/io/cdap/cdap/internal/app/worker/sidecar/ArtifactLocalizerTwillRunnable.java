/*
 * Copyright © 2021-2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.app.guice.DistributedArtifactManagerModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.KafkaClientModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.guice.KafkaLogAppenderModule;
import io.cdap.cdap.logging.guice.RemoteLogAppenderModule;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.guice.CoreSecurityModule;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * The {@link TwillRunnable} for running {@link ArtifactLocalizerService}.
 *
 * This runnable will run as a sidecar container for {@link io.cdap.cdap.internal.app.worker.TaskWorkerTwillRunnable}
 */
public class ArtifactLocalizerTwillRunnable extends AbstractTwillRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizerTwillRunnable.class);

  private ArtifactLocalizerService artifactLocalizerService;
  private LogAppenderInitializer logAppenderInitializer;
  private TokenManager tokenManager;

  public ArtifactLocalizerTwillRunnable(String cConfFileName, String hConfFileName) {
    super(ImmutableMap.of("cConf", cConfFileName, "hConf", hConfFileName));
  }

  @VisibleForTesting
  public static Injector createInjector(CConfiguration cConf, Configuration hConf) {
    List<Module> modules = new ArrayList<>();

    CoreSecurityModule coreSecurityModule = CoreSecurityRuntimeModule.getDistributedModule(cConf);

    modules.add(new ConfigModule(cConf, hConf));
    modules.add(new IOModule());
    modules.add(RemoteAuthenticatorModules.getDefaultModule());
    modules.add(new AuthenticationContextModules().getMasterModule());
    modules.add(coreSecurityModule);
    modules.add(new MessagingClientModule());
    modules.add(new MetricsClientRuntimeModule().getDistributedModules());

    // If MasterEnvironment is not available, assuming it is the old hadoop stack with ZK, Kafka
    MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();

    if (masterEnv == null) {
      modules.add(new ZKClientModule());
      modules.add(new ZKDiscoveryModule());
      modules.add(new KafkaClientModule());
      modules.add(new KafkaLogAppenderModule());
      modules.add(new DFSLocationModule());
    } else {
      modules.add(new AbstractModule() {
        @Override
        protected void configure() {
          bind(DiscoveryService.class)
            .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
          bind(DiscoveryServiceClient.class)
            .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));
        }
      });
      modules.add(new RemoteLogAppenderModule());
      modules.add(new LocalLocationModule());

      if (coreSecurityModule.requiresZKClient()) {
        modules.add(new ZKClientModule());
      }
    }
    modules.add(new DistributedArtifactManagerModule());

    return Guice.createInjector(modules);
  }

  @Override
  public void initialize(TwillContext context) {
    super.initialize(context);

    try {
      doInitialize();
    } catch (Exception e) {
      LOG.error("Encountered error while initializing ArtifactLocalizerTwillRunnable", e);
      Throwables.propagateIfPossible(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    CompletableFuture<Service.State> future = new CompletableFuture<>();
    artifactLocalizerService.addListener(new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        future.complete(from);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        future.completeExceptionally(failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    LOG.debug("Starting artifact localizer");
    artifactLocalizerService.start();

    try {
      Uninterruptibles.getUninterruptibly(future);
    } catch (ExecutionException e) {
      LOG.warn("Artifact localizer stopped with exception", e);
    }
  }

  @Override
  public void stop() {
    artifactLocalizerService.stop();
  }

  @Override
  public void destroy() {
    try {
      tokenManager.stopAndWait();
    } finally {
      logAppenderInitializer.close();
    }
  }

  @VisibleForTesting
  void doInitialize() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.clear();
    cConf.addResource(new File(getArgument("cConf")).toURI().toURL());

    Configuration hConf = new Configuration();
    hConf.clear();
    hConf.addResource(new File(getArgument("hConf")).toURI().toURL());

    Injector injector = createInjector(cConf, hConf);

    // Initialize logging context
    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    logAppenderInitializer.initialize();

    LoggingContext loggingContext = new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                              Constants.Logging.COMPONENT_NAME,
                                                              Constants.Service.ARTIFACT_LOCALIZER);
    LoggingContextAccessor.setLoggingContext(loggingContext);

    tokenManager = injector.getInstance(TokenManager.class);
    tokenManager.startAndWait();

    artifactLocalizerService = injector.getInstance(ArtifactLocalizerService.class);
  }
}
