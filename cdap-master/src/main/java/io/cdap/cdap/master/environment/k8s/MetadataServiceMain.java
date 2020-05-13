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

import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.cdap.cdap.app.guice.EntityVerifierModule;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.namespace.guice.NamespaceQueryAdminModule;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data2.audit.AuditModule;
import io.cdap.cdap.data2.metadata.writer.MessagingMetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metadata.MetadataService;
import io.cdap.cdap.metadata.MetadataServiceModule;
import io.cdap.cdap.metadata.MetadataSubscriberService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.spi.authorization.NoOpAuthorizer;
import io.cdap.cdap.security.spi.authorization.PrivilegesManager;
import io.cdap.cdap.spi.metadata.MetadataStorage;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * The main class to run metadata service. Also, the dataset op executor is running this process as well.
 */
public class MetadataServiceMain extends AbstractServiceMain<EnvironmentOptions> {

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(MetadataServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv, EnvironmentOptions options) {
    return Arrays.asList(
      new MessagingClientModule(),
      new NamespaceQueryAdminModule(),
      getDataFabricModule(),
      // Always use local table implementations, which use LevelDB.
      // In K8s, there won't be HBase and the cdap-site should be set to use SQL store for StructuredTable.
      new SystemDatasetRuntimeModule().getStandaloneModules(),
      new DataSetsModules().getStandaloneModules(),
      new MetadataServiceModule(),
      new AuditModule(),
      new EntityVerifierModule(),
      new AuthorizationEnforcementModule().getDistributedModules(),
      new AuthenticationContextModules().getMasterModule(),
      new DFSLocationModule(),
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
                             List<? super AutoCloseable> closeableResources,
                             MasterEnvironment masterEnv, MasterEnvironmentContext masterEnvContext,
                             EnvironmentOptions options) {
    services.add(injector.getInstance(MetadataService.class));
    services.add(injector.getInstance(MetadataSubscriberService.class));

    // Add a service just for closing MetadataStorage to release resource.
    // MetadataStorage is binded as Singleton, so ok to get the instance and close it.
    MetadataStorage metadataStorage = injector.getInstance(MetadataStorage.class);
    services.add(new AbstractService() {

      @Override
      protected void doStart() {
        notifyStarted();
      }

      @Override
      protected void doStop() {
        Closeables.closeQuietly(metadataStorage);
        notifyStopped();
      }
    });
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext(EnvironmentOptions options) {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                     Constants.Logging.COMPONENT_NAME,
                                     Constants.Service.METADATA_SERVICE);
  }
}
