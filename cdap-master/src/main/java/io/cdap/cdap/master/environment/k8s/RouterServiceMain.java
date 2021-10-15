/*
 * Copyright © 2019-2021 Cask Data, Inc.
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
import io.cdap.cdap.gateway.router.NettyRouter;
import io.cdap.cdap.gateway.router.RouterModules;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.guice.ExternalAuthenticationModule;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * The main class to run router service.
 */
public class RouterServiceMain extends AbstractServiceMain<EnvironmentOptions> {
  private static final Logger LOG = LoggerFactory.getLogger(RouterServiceMain.class);

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(RouterServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv,
                                           EnvironmentOptions options, CConfiguration cConf) {
    List<Module> modules = new ArrayList<>();

    modules.add(new MessagingClientModule());
    modules.add(new RouterModules().getDistributedModules());
    modules.add(new DFSLocationModule());
    modules.add(new ExternalAuthenticationModule());

    return modules;
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
    services.add(injector.getInstance(NettyRouter.class));
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    if (SecurityUtil.isInternalAuthEnabled(cConf)) {
      services.add(injector.getInstance(TokenManager.class));
    }
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext(EnvironmentOptions options) {
    return new ServiceLoggingContext(
        NamespaceId.SYSTEM.getNamespace(),
        Constants.Logging.COMPONENT_NAME,
        Constants.Service.GATEWAY);
  }
}
