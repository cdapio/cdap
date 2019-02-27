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

import co.cask.cdap.app.guice.EntityVerifierModule;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.DFSLocationModule;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.namespace.guice.NamespaceQueryAdminModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.data2.metadata.writer.MessagingMetadataPublisher;
import co.cask.cdap.data2.metadata.writer.MetadataPublisher;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.logging.guice.RemoteLogAppenderModule;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.metadata.MetadataService;
import co.cask.cdap.metadata.MetadataServiceModule;
import co.cask.cdap.metadata.MetadataSubscriberService;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.guice.SecureStoreClientModule;
import co.cask.cdap.security.impersonation.CurrentUGIProvider;
import co.cask.cdap.security.impersonation.DefaultOwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.spi.authorization.NoOpAuthorizer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * The main class to run metadata service. Also, the dataset op executor is running this process as well.
 */
public class MetadataServiceMain extends AbstractServiceMain {

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(MetadataServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules() {
    return Arrays.asList(
      new MessagingClientModule(),
      new NamespaceQueryAdminModule(),
      getDataFabricModule(),
      // Always use local table implementations, which use LevelDB.
      // In K8s, there won't be HBase and the cdap-site should be set to use SQL store for StructuredTable.
      new SystemDatasetRuntimeModule().getStandaloneModules(),
      new DataSetsModules().getStandaloneModules(),
      new ExploreClientModule(),
      new MetadataServiceModule(),
      new AuditModule(),
      new EntityVerifierModule(),
      new SecureStoreClientModule(),
      new AuthorizationEnforcementModule().getDistributedModules(),
      new AuthenticationContextModules().getMasterModule(),
      new DFSLocationModule(),
      new RemoteLogAppenderModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(Store.class).to(DefaultStore.class);

          // Current impersonation is not supported
          bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);
          bind(PrivilegesManager.class).to(NoOpAuthorizer.class);

          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
          // TODO (CDAP-14677): find a better way to inject metadata publisher
          bind(MetadataPublisher.class).to(MessagingMetadataPublisher.class);
        }
      }
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources) {
    services.add(injector.getInstance(MetadataService.class));
    services.add(injector.getInstance(MetadataSubscriberService.class));
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext() {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                     Constants.Logging.COMPONENT_NAME,
                                     Constants.Service.METADATA_SERVICE);
  }
}
