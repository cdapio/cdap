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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.DFSLocationModule;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.gateway.router.NettyRouter;
import co.cask.cdap.gateway.router.RouterModules;
import co.cask.cdap.logging.guice.RemoteLogAppenderModule;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.guice.SecurityModules;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * The main class to run router service.
 */
public class RouterServiceMain extends AbstractServiceMain {

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(RouterServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules() {
    return Arrays.asList(
      new MessagingClientModule(),
      new RouterModules().getDistributedModules(),
      new DFSLocationModule(),
      // Use the Standalone module for now, until we have proper support for key management for authentication in K8s
      new SecurityModules().getStandaloneModules(),
      new RemoteLogAppenderModule()
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources) {
    services.add(injector.getInstance(NettyRouter.class));
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext() {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                     Constants.Logging.COMPONENT_NAME,
                                     Constants.Service.GATEWAY);
  }
}
