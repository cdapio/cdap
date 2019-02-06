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

package co.cask.cdap.master.environment.k8s;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.namespace.guice.NamespaceQueryAdminModule;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.metrics.guice.MetricsHandlerModule;
import co.cask.cdap.metrics.guice.MetricsProcessorStatusServiceModule;
import co.cask.cdap.metrics.guice.MetricsStoreModule;
import co.cask.cdap.metrics.process.MessagingMetricsProcessorServiceFactory;
import co.cask.cdap.metrics.process.MetricsProcessorStatusService;
import co.cask.cdap.metrics.query.MetricsQueryService;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.assistedinject.FactoryModuleBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

/**
 * The main class to run metrics services, which includes both metrics processor and metrics query.
 */
public class MetricsServiceMain extends AbstractServiceMain {

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(MetricsServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules() {
    return Arrays.asList(
      new NamespaceQueryAdminModule(),
      new AuthorizationEnforcementModule().getDistributedModules(),
      new AuthenticationContextModules().getMasterModule(),
      new MessagingClientModule(),
      new SystemDatasetRuntimeModule().getStandaloneModules(),
      new MetricsStoreModule(),
      new FactoryModuleBuilder().build(MessagingMetricsProcessorServiceFactory.class),
      new MetricsProcessorStatusServiceModule(),
      new MetricsHandlerModule()
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources) {
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    Set<Integer> topicNumbers = IntStream.range(0, cConf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM))
      .boxed()
      .collect(Collectors.toSet());

    MetricsContext metricsContext = injector.getInstance(MetricsCollectionService.class)
      .getContext(Constants.Metrics.METRICS_PROCESSOR_CONTEXT);

    services.add(injector.getInstance(MessagingMetricsProcessorServiceFactory.class)
                   .create(topicNumbers, metricsContext, 0));
    services.add(injector.getInstance(MetricsProcessorStatusService.class));
    services.add(injector.getInstance(MetricsQueryService.class));
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext() {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                     Constants.Logging.COMPONENT_NAME,
                                     Constants.Service.METRICS);
  }
}
