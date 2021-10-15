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
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.messaging.server.MessagingHttpService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Main class for running {@link MessagingService} in Kubernetes.
 */
public class MessagingServiceMain extends AbstractServiceMain<EnvironmentOptions> {

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(MessagingServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv,
                                           EnvironmentOptions options, CConfiguration cConf) {
    // We use the "local" module in K8s, as PV will be used as the persistent storage.
    return Arrays.asList(
      new NamespaceQueryAdminModule(),
      new AuthorizationEnforcementModule().getDistributedModules(),
      new MessagingServerRuntimeModule().getStandaloneModules(),
      new DFSLocationModule()
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources,
                             MasterEnvironment masterEnv, MasterEnvironmentContext masterEnvContext,
                             EnvironmentOptions options) {
    MessagingService messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      services.add((Service) messagingService);
    }
    services.add(injector.getInstance(MessagingHttpService.class));
    Binding<ZKClientService> zkBinding = injector.getExistingBinding(Key.get(ZKClientService.class));
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
                                     Constants.Service.MESSAGING_SERVICE);
  }
}
