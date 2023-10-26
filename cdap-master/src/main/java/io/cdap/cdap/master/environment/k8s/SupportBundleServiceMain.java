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
import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.namespace.guice.NamespaceQueryAdminModule;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.guice.MessagingServiceModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.support.guice.SupportBundleServiceModule;
import io.cdap.cdap.support.services.SupportBundleInternalService;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.twill.zookeeper.ZKClientService;

/**
 * The main class to run supportbundle and other supporting services.
 */

public class SupportBundleServiceMain extends AbstractServiceMain<EnvironmentOptions> {

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(SupportBundleServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv,
      EnvironmentOptions options, CConfiguration cConf) {
    return Arrays.asList(
        new MessagingServiceModule(cConf),
        new NamespaceQueryAdminModule(),
        getDataFabricModule(),
        // Always use local table implementations, which use LevelDB.
        // In K8s, there won't be HBase and the cdap-site should be set to use SQL store for StructuredTable.
        new SystemDatasetRuntimeModule().getStandaloneModules(),
        // The Dataset set modules are only needed to satisfy dependency injection
        new DataSetsModules().getStandaloneModules(),
        new AuthorizationEnforcementModule().getDistributedModules(),
        new SupportBundleServiceModule(),
        new DFSLocationModule()
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
      List<? super AutoCloseable> closeableResources,
      MasterEnvironment masterEnv, MasterEnvironmentContext masterEnvContext,
      EnvironmentOptions options) {
    services.add(injector.getInstance(SupportBundleInternalService.class));
    Binding<ZKClientService> zkBinding = injector.getExistingBinding(
        Key.get(ZKClientService.class));
    if (zkBinding != null) {
      services.add(zkBinding.getProvider().get());
    }
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext(EnvironmentOptions options) {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
        Constants.Logging.COMPONENT_NAME,
        Constants.Service.SUPPORT_BUNDLE_SERVICE);
  }

}
