/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.data.runtime.main;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.app.guice.EntityVerifierModule;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.KafkaClientModule;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.namespace.guice.NamespaceQueryAdminModule;
import io.cdap.cdap.common.twill.AbstractMasterTwillRunnable;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.audit.AuditModule;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.metadata.writer.DefaultMetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.MessagingMetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.guice.KafkaLogAppenderModule;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metadata.MetadataService;
import io.cdap.cdap.metadata.MetadataServiceModule;
import io.cdap.cdap.metadata.MetadataSubscriberService;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.RemotePermissionManager;
import io.cdap.cdap.security.guice.SecureStoreClientModule;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.RemoteUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.spi.authorization.PermissionManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillContext;

import java.util.List;

/**
 * Executes user code on behalf of a particular user inside a YARN container, for security.
 */
public class DatasetOpExecutorServerTwillRunnable extends AbstractMasterTwillRunnable {

  private Injector injector;

  public DatasetOpExecutorServerTwillRunnable(String name, String cConfName, String hConfName) {
    super(name, cConfName, hConfName);
  }

  @Override
  protected Injector doInit(TwillContext context) {
    CConfiguration cConf = getCConfiguration();
    Configuration hConf = getConfiguration();

    // Set the host name to the one provided by Twill
    cConf.set(Constants.Dataset.Executor.ADDRESS, context.getHost().getHostName());
    cConf.set(Constants.Metadata.SERVICE_BIND_ADDRESS, context.getHost().getHostName());

    String txClientId = String.format("cdap.service.%s.%d", Constants.Service.DATASET_EXECUTOR,
                                      context.getInstanceId());
    injector = createInjector(cConf, hConf, txClientId);

    injector.getInstance(LogAppenderInitializer.class).initialize();
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.DATASET_EXECUTOR));
    return injector;
  }

  @VisibleForTesting
  static Injector createInjector(CConfiguration cConf, Configuration hConf, String txClientId) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new ZKDiscoveryModule(),
      new KafkaClientModule(),
      new MessagingClientModule(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new DFSLocationModule(),
      new NamespaceQueryAdminModule(),
      new DataFabricModules(txClientId).getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new DataSetServiceModules().getDistributedModules(),
      new KafkaLogAppenderModule(),
      new ExploreClientModule(),
      new MetadataServiceModule(),
      new AuditModule(),
      new EntityVerifierModule(),
      new SecureStoreClientModule(),
      new AuthorizationEnforcementModule().getDistributedModules(),
      new AuthenticationContextModules().getMasterModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(Store.class).to(DefaultStore.class);
          bind(UGIProvider.class).to(RemoteUGIProvider.class).in(Scopes.SINGLETON);
          bind(PermissionManager.class).to(RemotePermissionManager.class);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
          // TODO (CDAP-14677): find a better way to inject metadata publisher
          bind(MetadataPublisher.class).to(MessagingMetadataPublisher.class);
          bind(MetadataServiceClient.class).to(DefaultMetadataServiceClient.class);
        }
      });
  }

  @Override
  protected void addServices(List<? super Service> services) {
    services.add(injector.getInstance(DatasetOpExecutorService.class));
    services.add(injector.getInstance(MetadataService.class));
    services.add(injector.getInstance(MetadataSubscriberService.class));
  }
}
