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
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.AuthorizationModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.UnsupportedExploreClient;
import co.cask.cdap.app.preview.PreviewHttpModule;
import co.cask.cdap.app.preview.PreviewHttpServer;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.DFSLocationModule;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.logging.guice.RemoteLogAppenderModule;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.metadata.MetadataReaderWriterModules;
import co.cask.cdap.metadata.MetadataServiceModule;
import co.cask.cdap.metrics.guice.MetricsStoreModule;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.guice.SecureStoreServerModule;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * The main class to run the preview service.
 */
public class PreviewServiceMain extends AbstractServiceMain {

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(PreviewServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules() {
    return Arrays.asList(
      new PreviewHttpModule().getDistributedModules(),
      new DataSetServiceModules().getStandaloneModules(),
      new DataSetsModules().getStandaloneModules(),
      new AppFabricServiceRuntimeModule().getStandaloneModules(),
      new ProgramRunnerRuntimeModule().getStandaloneModules(),
      new MetricsStoreModule(),
      new MessagingClientModule(),
      new AuditModule(),
      new SecureStoreServerModule(),
      new MetadataReaderWriterModules().getStandaloneModules(),
      getDataFabricModule(),
      new MetadataServiceModule(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getMasterModule(),
      new RemoteLogAppenderModule(),
      new DFSLocationModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(ExploreClient.class).to(UnsupportedExploreClient.class);
        }
      }
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources) {
    services.add(injector.getInstance(MetricsCollectionService.class));
    services.add(injector.getInstance(PreviewHttpServer.class));
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext() {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                     Constants.Logging.COMPONENT_NAME,
                                     Constants.Service.PREVIEW_HTTP);
  }
}
