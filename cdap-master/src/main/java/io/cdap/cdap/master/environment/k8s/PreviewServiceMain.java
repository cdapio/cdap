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
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.guice.UnsupportedExploreClient;
import io.cdap.cdap.app.preview.PreviewHttpModule;
import io.cdap.cdap.app.preview.PreviewHttpServer;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.audit.AuditModule;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.internal.app.preview.PreviewRunStopper;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metadata.MetadataReaderWriterModules;
import io.cdap.cdap.metadata.MetadataServiceModule;
import io.cdap.cdap.metrics.guice.MetricsStoreModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.SecureStoreClientModule;
import org.apache.twill.api.Configs;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * The main class to run the preview service.
 */
public class PreviewServiceMain extends AbstractServiceMain<EnvironmentOptions> {
  private static final String PREVIEW_RUNNER_CPU_MULTIPLIER = "preview.runner.container.cpu.multiplier";
  private static final String PREVIEW_RUNNER_MEMORY_MULTIPLIER = "preview.runner.container.memory.multiplier";
  private static final String PREVIEW_RUNNER_HEAP_RESERVED_RATIO = "preview.runner.container.java.heap.memory.ratio";

  // Following constants are same as defined in AbstractKubeTwillPreparer
  private static final String CPU_MULTIPLIER = "master.environment.k8s.container.cpu.multiplier";
  private static final String MEMORY_MULTIPLIER = "master.environment.k8s.container.memory.multiplier";

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(PreviewServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv, EnvironmentOptions options) {
    return Arrays.asList(
      new PreviewHttpModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(PreviewRunStopper.class).to(DistributedPreviewRunnerServiceStopper.class).in(Scopes.SINGLETON);
        }
      },
      new DataSetServiceModules().getStandaloneModules(),
      new DataSetsModules().getStandaloneModules(),
      new AppFabricServiceRuntimeModule().getStandaloneModules(),
      new ProgramRunnerRuntimeModule().getStandaloneModules(),
      new MetricsStoreModule(),
      new MessagingClientModule(),
      new AuditModule(),
      new SecureStoreClientModule(),
      new MetadataReaderWriterModules().getStandaloneModules(),
      getDataFabricModule(),
      new DFSLocationModule(),
      new MetadataServiceModule(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getMasterModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(TwillRunnerService.class).toProvider(
            new SupplierProviderBridge<>(masterEnv.getTwillRunnerSupplier())).in(Scopes.SINGLETON);
          bind(TwillRunner.class).to(TwillRunnerService.class);
          bind(ExploreClient.class).to(UnsupportedExploreClient.class);
        }
      }
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources,
                             MasterEnvironment masterEnv, MasterEnvironmentContext masterEnvContext,
                             EnvironmentOptions options) {
    services.add(injector.getInstance(MetricsCollectionService.class));
    services.add(injector.getInstance(PreviewHttpServer.class));
    TwillRunnerService previewRunner = injector.getInstance(TwillRunnerService.class);
    previewRunner.start();
    Map<String, String> configurations = masterEnvContext.getConfigurations();
    if (configurations.containsKey(PREVIEW_RUNNER_CPU_MULTIPLIER)) {
      configurations.put(CPU_MULTIPLIER, configurations.get(PREVIEW_RUNNER_CPU_MULTIPLIER));
    }
    if (configurations.containsKey(PREVIEW_RUNNER_MEMORY_MULTIPLIER)) {
      configurations.put(MEMORY_MULTIPLIER, configurations.get(PREVIEW_RUNNER_MEMORY_MULTIPLIER));
    }
    if (configurations.containsKey(PREVIEW_RUNNER_HEAP_RESERVED_RATIO)) {
      configurations.put(Configs.Keys.HEAP_RESERVED_MIN_RATIO, configurations.get(PREVIEW_RUNNER_HEAP_RESERVED_RATIO));
    }
    ResourceSpecification specification = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(256, ResourceSpecification.SizeUnit.MEGA)
      .build();
    previewRunner.prepare(new PreviewRunnerTwillRunnable(), specification).start(300000, TimeUnit.MILLISECONDS);
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext(EnvironmentOptions options) {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                     Constants.Logging.COMPONENT_NAME,
                                     Constants.Service.PREVIEW_HTTP);
  }
}
