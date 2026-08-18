/*
 * Copyright © 2026 Cask Data, Inc.
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
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.TaskManagerService;
import io.cdap.cdap.common.internal.remote.TaskManagerServiceModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.data2.audit.AuditModule;
import io.cdap.cdap.messaging.guice.MessagingServiceModule;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Main entry point for the standalone Task Manager Service (Netty Proxy) in Kubernetes.
 *
 * <p>Lifecycle & Architecture:
 * <ul>
 *   <li>Bootstrapped as an independent Master container pod ({@code cdap-taskmanager}) in GKE.</li>
 *   <li>Configures Guice dependency injection with {@link TaskManagerServiceModule} to start the
 *       underlying {@link TaskManagerService} (Netty Proxy HTTP server).</li>
 *   <li>Wires standard CDAP logging context under {@code task-manager} for Cloud Logging.</li>
 * </ul>
 */
public class TaskManagerMain extends AbstractServiceMain<EnvironmentOptions> {

  public static void main(String[] args) throws Exception {
    main(TaskManagerMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv,
                                           EnvironmentOptions options,
                                           CConfiguration cConf) {
    return Arrays.asList(
        new MessagingServiceModule(cConf),
        new AuditModule(),
        getDataFabricModule(),
        new TaskManagerServiceModule()
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources,
                             MasterEnvironment masterEnv,
                             MasterEnvironmentContext masterEnvContext,
                             EnvironmentOptions options) {
    services.add(injector.getInstance(TaskManagerService.class));
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext(EnvironmentOptions options) {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
        Constants.Logging.COMPONENT_NAME,
        "task-manager");
  }
}
