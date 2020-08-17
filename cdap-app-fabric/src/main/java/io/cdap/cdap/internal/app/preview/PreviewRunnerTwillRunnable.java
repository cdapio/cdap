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

package io.cdap.cdap.internal.app.preview;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.guice.UnsupportedExploreClient;
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
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.data.runtime.ConstantTransactionSystemClient;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.audit.AuditModule;
import io.cdap.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import io.cdap.cdap.data2.transaction.TransactionSystemClientService;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.internal.app.runtime.k8s.PreviewRequestPollerInfo;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.guice.KafkaLogAppenderModule;
import io.cdap.cdap.logging.guice.RemoteLogAppenderModule;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.twill.ExtendedTwillContext;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metadata.MetadataReaderWriterModules;
import io.cdap.cdap.metadata.MetadataServiceModule;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.metrics.guice.MetricsStoreModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.SecureStoreClientModule;
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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * The {@link TwillRunnable} for running {@link PreviewRunner}.
 */
public class PreviewRunnerTwillRunnable extends AbstractTwillRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(PreviewRunnerTwillRunnable.class);

  private PreviewRunnerManager previewRunnerManager;
  private LogAppenderInitializer logAppenderInitializer;

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
    previewRunnerManager.stop();
  }

  @Override
  public void destroy() {
    logAppenderInitializer.close();
  }

  private void doInitialize(TwillContext context) throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.clear();
    cConf.addResource(new File(getArgument("cConf")).toURI().toURL());

    Configuration hConf = new Configuration();
    hConf.clear();
    hConf.addResource(new File(getArgument("hConf")).toURI().toURL());

    PreviewRequestPollerInfo pollerInfo;
    if (context instanceof ExtendedTwillContext) {
      pollerInfo = new PreviewRequestPollerInfo(context.getInstanceId(), ((ExtendedTwillContext) context).getUID());
    } else {
      pollerInfo = new PreviewRequestPollerInfo(context.getInstanceId(), null);
    }
    LOG.debug("Initializing preview runner with poller info {}", pollerInfo);

    Injector injector = createInjector(cConf, hConf, pollerInfo);

    // Initialize logging context
    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    logAppenderInitializer.initialize();

    LoggingContext loggingContext = new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                              Constants.Logging.COMPONENT_NAME,
                                                              PreviewRunnerTwillApplication.NAME);
    LoggingContextAccessor.setLoggingContext(loggingContext);
    previewRunnerManager = injector.getInstance(PreviewRunnerManager.class);
  }

  @VisibleForTesting
  static Injector createInjector(CConfiguration cConf, Configuration hConf, PreviewRequestPollerInfo pollerInfo) {
    List<Module> modules = new ArrayList<>();

    byte[] pollerInfoBytes = Bytes.toBytes(new Gson().toJson(pollerInfo));
    SConfiguration sConf = SConfiguration.create();

    modules.add(new ConfigModule(cConf, hConf, sConf));
    modules.add(new PreviewConfigModule(cConf, hConf, sConf));
    modules.add(new IOModule());
    modules.add(new MetricsClientRuntimeModule().getDistributedModules());

    // If MasterEnvironment is not available, assuming it is the old hadoop stack with ZK, Kafka
    MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();

    if (masterEnv == null) {
      modules.add(new ZKClientModule());
      modules.add(new ZKDiscoveryModule());
      modules.add(new KafkaClientModule());
      modules.add(new KafkaLogAppenderModule());
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
    }

    modules.add(new PreviewRunnerManagerModule().getDistributedModules());
    modules.add(new DataSetServiceModules().getStandaloneModules());
    modules.add(new DataSetsModules().getStandaloneModules());
    modules.add(new AppFabricServiceRuntimeModule().getStandaloneModules());
    modules.add(new ProgramRunnerRuntimeModule().getStandaloneModules());
    modules.add(new MetricsStoreModule());
    modules.add(new MessagingClientModule());
    modules.add(new AuditModule());
    modules.add(new SecureStoreClientModule());
    modules.add(new MetadataReaderWriterModules().getStandaloneModules());
    modules.add(new DFSLocationModule());
    modules.add(new MetadataServiceModule());
    modules.add(new AuthorizationModule());
    modules.add(new AuthorizationEnforcementModule().getMasterModule());
    modules.add(Modules.override(
      new DataFabricModules("master").getDistributedModules()).with(new AbstractModule() {
      @Override
      protected void configure() {
        // Bind transaction system to a constant one, basically no transaction, with every write become
        // visible immediately.
        // TODO: Ideally we shouldn't need this at all. However, it is needed now to satisfy dependencies
        bind(TransactionSystemClientService.class).to(DelegatingTransactionSystemClientService.class);
        bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class);

        bind(ExploreClient.class).to(UnsupportedExploreClient.class);
        bind(PreviewRequestPollerInfoProvider.class).toInstance(() -> pollerInfoBytes);
      }
    }));


    return Guice.createInjector(modules);
  }
}
