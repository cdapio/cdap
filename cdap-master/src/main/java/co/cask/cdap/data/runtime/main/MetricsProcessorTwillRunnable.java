/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data.runtime.main;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.twill.AbstractMasterTwillRunnable;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsProcessorStatusServiceModule;
import co.cask.cdap.metrics.guice.MetricsStoreModule;
import co.cask.cdap.metrics.process.MessagingMetricsProcessorServiceFactory;
import co.cask.cdap.metrics.process.MetricsProcessorStatusService;
import co.cask.cdap.metrics.runtime.MessagingMetricsProcessorRuntimeService;
import co.cask.cdap.metrics.store.DefaultMetricDatasetFactory;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.impersonation.DefaultOwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.impersonation.UnsupportedUGIProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Twill Runnable to run MetricsProcessor in YARN.
 */
public final class MetricsProcessorTwillRunnable extends AbstractMasterTwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsProcessorTwillRunnable.class);

  private Injector injector;

  public MetricsProcessorTwillRunnable(String name, String cConfName, String hConfName) {
    super(name, cConfName, hConfName);
  }

  @Override
  protected Injector doInit(TwillContext context) {
    getCConfiguration().set(Constants.MetricsProcessor.ADDRESS, context.getHost().getCanonicalHostName());
    // Set the hostname of the machine so that cConf can be used to start internal services
    LOG.info("{} Setting host name to {}", name, context.getHost().getCanonicalHostName());

    String txClientId = String.format("cdap.service.%s.%d", Constants.Service.METRICS_PROCESSOR,
                                      context.getInstanceId());
    injector = createGuiceInjector(getCConfiguration(), getConfiguration(), txClientId, context);

    injector.getInstance(LogAppenderInitializer.class).initialize();
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.METRICS_PROCESSOR));
    return injector;
  }

  @Override
  public void addServices(List<? super Service> services) {
    services.add(injector.getInstance(MessagingMetricsProcessorRuntimeService.class));
    services.add(injector.getInstance(MetricsProcessorStatusService.class));
  }

  @VisibleForTesting
  static Injector createGuiceInjector(CConfiguration cConf, Configuration hConf, String txClientId,
                                      TwillContext twillContext) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new MessagingClientModule(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new MetricsStoreModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new LoggingModules().getDistributedModules(),
      new LocationRuntimeModule().getDistributedModules(),
      new NamespaceClientRuntimeModule().getDistributedModules(),
      new DataFabricModules(txClientId).getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new MetricsProcessorModule(twillContext),
      new MetricsProcessorStatusServiceModule(),
      new AuditModule().getDistributedModules(),
      new AuthorizationEnforcementModule().getDistributedModules(),
      new AuthenticationContextModules().getMasterModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // MetricsProcessor should never need to use UGIProvider. It is simply bound in HBaseQueueAdmin
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class).in(Scopes.SINGLETON);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
        }
      }
    );
  }

  static final class MetricsProcessorModule extends PrivateModule {
    final Integer instanceId;

    MetricsProcessorModule(TwillContext twillContext) {
      this.instanceId = twillContext.getInstanceId();
    }

    @Override
    protected void configure() {
      // Single MetricDatasetFactory used by both KafkaMetricsProcessorService and MessagingMetricsProcessorService
      bind(MetricDatasetFactory.class).to(DefaultMetricDatasetFactory.class).in(Scopes.SINGLETON);
      bind(Integer.class).annotatedWith(Names.named(Constants.Metrics.TWILL_INSTANCE_ID)).toInstance(instanceId);

      install(new FactoryModuleBuilder()
                .build(MessagingMetricsProcessorServiceFactory.class));

      bind(MessagingMetricsProcessorRuntimeService.class);
      expose(MessagingMetricsProcessorRuntimeService.class);
    }

    @SuppressWarnings("unused")
    @Provides
    @Named(Constants.Metrics.TOPIC_PREFIX)
    public String providesTopicPrefix(CConfiguration cConf) {
      return cConf.get(Constants.Metrics.TOPIC_PREFIX);
    }

    @SuppressWarnings("unused")
    @Provides
    @Named(Constants.Metrics.MESSAGING_TOPIC_NUM)
    public int providesMessagingTopicPartition(CConfiguration cConf) {
      return cConf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM);
    }

    @SuppressWarnings("unused")
    @Provides
    @Named(Constants.Metrics.PROCESSOR_MAX_DELAY_MS)
    public long providesMadDelay(CConfiguration cConf) {
      return cConf.getLong(Constants.Metrics.PROCESSOR_MAX_DELAY_MS);
    }

    @SuppressWarnings("unused")
    @Provides
    @Named(Constants.Metrics.QUEUE_SIZE)
    public int providesQueueSize(CConfiguration cConf) {
      return cConf.getInt(Constants.Metrics.QUEUE_SIZE);
    }
  }
}
