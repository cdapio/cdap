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
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.guice.SecurityModule;
import io.cdap.cdap.security.guice.SecurityModules;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.server.ExternalAuthenticationServer;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.ArrayList;
import java.util.List;

/**
 * The main class responsible for Authentication  .
 */
public class AuthenticationServiceMain extends AbstractServiceMain<EnvironmentOptions> {

  public static void main(String[] args) throws Exception {
    main(AuthenticationServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv,
                                           EnvironmentOptions options,
                                           CConfiguration cConf) {

    if (!SecurityUtil.isManagedSecurity(cConf)) {
      throw new RuntimeException("Security is not enabled. Authentication service shouldn't be used");
    }

    List<Module> modules = new ArrayList<>();
    modules.add(new MessagingClientModule());

    SecurityModule securityModule = SecurityModules.getDistributedModule(cConf);
    modules.add(securityModule);
    if (securityModule.requiresZKClient()) {
      modules.add(new ZKClientModule());
    }

    return modules;
  }

  @Override
  protected void addServices(Injector injector,
                             List<? super Service> services,
                             List<? super AutoCloseable> closeableResources,
                             MasterEnvironment masterEnv,
                             MasterEnvironmentContext masterEnvContext,
                             EnvironmentOptions options) {
    Binding<ZKClientService> zkBinding = injector.getExistingBinding(Key.get(ZKClientService.class));
    if (zkBinding != null) {
      services.add(zkBinding.getProvider().get());
    }
    services.add(injector.getInstance(ExternalAuthenticationServer.class));
  }

  @Override
  protected LoggingContext getLoggingContext(EnvironmentOptions options) {
    return new ServiceLoggingContext(
        NamespaceId.SYSTEM.getNamespace(),
        Constants.Logging.COMPONENT_NAME,
        Constants.Service.AUTHENTICATION);
  }
}
