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
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.SupportBundleServiceModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.namespace.guice.NamespaceQueryAdminModule;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.internal.app.services.SupportBundleInternalService;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.store.SecureStoreService;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * The main class to run app-fabric and other supporting services.
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
      new MessagingClientModule(),
      new NamespaceQueryAdminModule(),
      getDataFabricModule(),
      // Always use local table implementations, which use LevelDB.
      // In K8s, there won't be HBase and the cdap-site should be set to use SQL store for StructuredTable.
      new SystemDatasetRuntimeModule().getStandaloneModules(),
      // The Dataset set modules are only needed to satisfy dependency injection
      new DataSetsModules().getStandaloneModules(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getMasterModule(),
      new SupportBundleServiceModule(),
      new SecureStoreServerModule(),
      new DFSLocationModule()
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources,
                             MasterEnvironment masterEnv, MasterEnvironmentContext masterEnvContext,
                             EnvironmentOptions options) {
    services.add(injector.getInstance(SupportBundleInternalService.class));
    services.add(injector.getInstance(SecureStoreService.class));
    Binding<ZKClientService> zkBinding = injector.getExistingBinding(
      Key.get(ZKClientService.class));
    if (zkBinding != null) {
      services.add(zkBinding.getProvider().get());
    }

    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    if (SecurityUtil.isInternalAuthEnabled(cConf)) {
      services.add(injector.getInstance(TokenManager.class));
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
