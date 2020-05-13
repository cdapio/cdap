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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.app.guice.RuntimeServerModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeProgramStatusSubscriberService;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeServer;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.store.StoreDefinition;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * The main class to run the runtime service for program runtime monitoring.
 */
public class RuntimeServiceMain extends AbstractServiceMain<EnvironmentOptions> {

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(RuntimeServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv, EnvironmentOptions options) {
    return Arrays.asList(
      new MessagingClientModule(),
      new SystemDatasetRuntimeModule().getStandaloneModules(),
      getDataFabricModule(),
      new RuntimeServerModule()
    );
  }

  @Override
  protected CConfiguration updateCConf(CConfiguration cConf) {
    // Runtime service always maintain a storage locally for replicating program states from TMS.
    cConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_NOSQL);
    return cConf;
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources,
                             MasterEnvironment masterEnv, MasterEnvironmentContext masterEnvContext,
                             EnvironmentOptions options) {
    services.add(new AbstractIdleService() {
      @Override
      protected void startUp() throws Exception {
        try {
          StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class),
                                          injector.getInstance(StructuredTableRegistry.class));
        } catch (TableAlreadyExistsException e) {
          // ignore
        }
      }

      @Override
      protected void shutDown() {
        // no-op
      }
    });
    services.add(injector.getInstance(RuntimeProgramStatusSubscriberService.class));
    services.add(injector.getInstance(RuntimeServer.class));
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext(EnvironmentOptions options) {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                     Constants.Logging.COMPONENT_NAME,
                                     Constants.Service.RUNTIME);
  }
}
