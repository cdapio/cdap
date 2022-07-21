/*
 * Copyright © 2014-2022 Cask Data, Inc.
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

package io.cdap.cdap.security.runtime;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.ServiceBindException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
import io.cdap.cdap.common.runtime.DaemonMain;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.guice.ExternalAuthenticationModule;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.server.ExternalAuthenticationServer;
import org.apache.twill.internal.Services;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Server for authenticating clients accessing CDAP.  When a client authenticates successfully, it is issued
 * an access token containing a verifiable representation of the client's identity.  Other CDAP services
 * (such as the router) can independently verify client identities based on the token contents.
 */
public class AuthenticationServerMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationServerMain.class);
  private ZKClientService zkClientService;
  private ExternalAuthenticationServer authServer;
  private CConfiguration configuration;

  @Override
  public void init(String[] args) {
    Injector injector = Guice.createInjector(new ConfigModule(),
                                             new IOModule(),
                                             RemoteAuthenticatorModules.getDefaultModule(),
                                             new ZKClientModule(),
                                             new ZKDiscoveryModule(),
                                             new CoreSecurityRuntimeModule().getDistributedModules(),
                                             new ExternalAuthenticationModule());
    configuration = injector.getInstance(CConfiguration.class);

    if (SecurityUtil.isManagedSecurity(configuration)) {
      this.zkClientService = injector.getInstance(ZKClientService.class);
      this.authServer = injector.getInstance(ExternalAuthenticationServer.class);
    }
  }

  @Override
  public void start() throws Exception {
    if (authServer != null) {
      try {
        LOG.info("Starting AuthenticationServer.");

        // Enable Kerberos login
        SecurityUtil.enableKerberosLogin(configuration);

        io.cdap.cdap.common.service.Services.startAndWait(zkClientService,
                                                          configuration.getLong(
                                                            Constants.Zookeeper.CLIENT_STARTUP_TIMEOUT_MILLIS),
                                                          TimeUnit.MILLISECONDS,
                                                          String.format("Connection timed out while trying to start " +
                                                                          "ZooKeeper client. Please verify that the " +
                                                                          "ZooKeeper quorum settings are correct in " +
                                                                          "cdap-site.xml. Currently configured as: %s",
                                                                        zkClientService.getConnectString()));
        authServer.startAndWait();
      } catch (Exception e) {
        Throwable rootCause = Throwables.getRootCause(e);
        if (rootCause instanceof ServiceBindException) {
          LOG.error("Failed to start Authentication Server: {}", rootCause.getMessage());
        } else {
          LOG.error("Failed to start Authentication Server", e);
        }
      }
    } else {
      String warning = "AuthenticationServer not started since security is disabled." +
                        " To enable security, set \"security.enabled\" = \"true\" in cdap-site.xml" +
                        " and edit the appropriate configuration.";
      LOG.warn(warning);
    }
  }

  @Override
  public void stop() {
    if (authServer != null) {
      LOG.info("Stopping AuthenticationServer.");
      Futures.getUnchecked(Services.chainStop(authServer, zkClientService));
    }
  }

  @Override
  public void destroy() {
  }

  public static void main(String[] args) throws Exception {
    new AuthenticationServerMain().doMain(args);
  }
}
