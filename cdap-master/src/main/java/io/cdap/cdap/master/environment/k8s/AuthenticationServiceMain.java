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

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.common.ServiceBindException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.guice.SecurityModules;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.server.ExternalAuthenticationServer;
import org.apache.twill.internal.Services;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The main class responsible for Authentication  .
 */
public class AuthenticationServiceMain extends AbstractServiceMain<EnvironmentOptions> {

  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationServiceMain.class);

  public static void main(String[] args) throws Exception {
    main(AuthenticationServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules(
      MasterEnvironment masterEnv, EnvironmentOptions options, CConfiguration cConf) {
    return Arrays.asList(
        getDataFabricModule(),
        new ZKClientModule(),
        new SecurityModules().getDistributedModules(),
        new MessagingClientModule());
  }

  @Override
  protected void addServices(
      Injector injector, List<? super Service> services, List<? super AutoCloseable> closeableResources,
      MasterEnvironment masterEnv, MasterEnvironmentContext masterEnvContext, EnvironmentOptions options) {

    MessagingService messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      services.add((Service) messagingService);
    }

    CConfiguration configuration = injector.getInstance(CConfiguration.class);

    if (configuration.getBoolean(Constants.Security.ENABLED)) {

      services.add(new AbstractService() {

        @Override
        protected void doStart() {

          ZKClientService zkClientService = injector.getInstance(ZKClientService.class);
          ExternalAuthenticationServer authServer = injector.getInstance(ExternalAuthenticationServer.class);

          try {
            LOG.info("Starting AuthenticationServer.");

            // Enable Kerberos login
            SecurityUtil.enableKerberosLogin(configuration);

            io.cdap.cdap.common.service.Services.startAndWait(
                zkClientService,
                configuration.getLong(
                    Constants.Zookeeper.CLIENT_STARTUP_TIMEOUT_MILLIS),
                TimeUnit.MILLISECONDS,
                String.format(
                    "Connection timed out while trying to start " +
                        "ZooKeeper client. Please verify that the " +
                        "ZooKeeper quorum settings are correct in " +
                        "cdap-site.xml. Currently configured as: %s",
                    configuration.get(Constants.Zookeeper.QUORUM)));
            authServer.startAndWait();

            notifyStarted();
          } catch (Exception e) {
            Throwable rootCause = Throwables.getRootCause(e);
            if (rootCause instanceof ServiceBindException) {
              LOG.error("Failed to start Authentication Server: {}", rootCause.getMessage());
            } else {
              LOG.error("Failed to start Authentication Server", e);
            }
          }
        }

        @Override
        protected void doStop() {

          ZKClientService zkClientService = injector.getInstance(ZKClientService.class);
          ExternalAuthenticationServer authServer = injector.getInstance(ExternalAuthenticationServer.class);

          LOG.info("Stopping AuthenticationServer.");
          Futures.getUnchecked(Services.chainStop(authServer, zkClientService));

          notifyStopped();
        }
      });

      services.add(injector.getInstance(ZKClientService.class));
      services.add(injector.getInstance(ExternalAuthenticationServer.class));
    } else {
      String warning = "AuthenticationServer not started since security is disabled." +
          " To enable security, set \"security.enabled\" = \"true\" in cdap-site.xml" +
          " and edit the appropriate configuration.";
      LOG.warn(warning);
    }
  }

  @Override
  protected LoggingContext getLoggingContext(
      EnvironmentOptions options) {
    return new ServiceLoggingContext(
        NamespaceId.SYSTEM.getNamespace(),
        Constants.Logging.COMPONENT_NAME,
        Constants.Service.AUTHENTICATION);
  }
}
