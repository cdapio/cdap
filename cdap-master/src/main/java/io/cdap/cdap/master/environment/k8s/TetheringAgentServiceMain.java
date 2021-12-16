/*
 * Copyright © 2021 Cask Data, Inc.
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
import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.NamespaceAdminModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.internal.tethering.TetheringAgentService;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.metrics.collect.LocalMetricsCollectionService;
import io.cdap.cdap.proto.id.NamespaceId;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Main class for running remote agent service in Kubernetes.
 */
public class
TetheringAgentServiceMain extends AbstractServiceMain<EnvironmentOptions> {
  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(TetheringAgentServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv,
                                           EnvironmentOptions options, CConfiguration cConf) {
    return Arrays.asList(
      new ConfigModule(cConf),
      new SystemDatasetRuntimeModule().getStandaloneModules(),
      new TransactionModules().getSingleNodeModules(),
      new NamespaceAdminModule().getStandaloneModules(),
      new StorageModule(),
      new PrivateModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(LocalMetricsCollectionService.class).in(Scopes.SINGLETON);
          expose(MetricsCollectionService.class);
          bind(TetheringAgentService.class).in(Scopes.SINGLETON);
          expose(TetheringAgentService.class);
        }
      });
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources,
                             MasterEnvironment masterEnv, MasterEnvironmentContext masterEnvContext,
                             EnvironmentOptions options) {
    Binding<ZKClientService> zkBinding = injector.getExistingBinding(Key.get(ZKClientService.class));
    if (zkBinding != null) {
      services.add(zkBinding.getProvider().get());
    }
    services.add(injector.getInstance(TetheringAgentService.class));
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext(EnvironmentOptions options) {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                     Constants.Logging.COMPONENT_NAME,
                                     Constants.Service.REMOTE_AGENT_SERVICE);
  }
}
