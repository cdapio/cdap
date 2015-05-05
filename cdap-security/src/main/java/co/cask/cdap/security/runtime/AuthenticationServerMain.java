/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.security.runtime;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.common.runtime.DaemonMain;
import co.cask.cdap.security.guice.SecurityModules;
import co.cask.cdap.security.server.ExternalAuthenticationServer;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.common.Services;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                                             new SecurityModules().getDistributedModules(),
                                             new DiscoveryRuntimeModule().getDistributedModules(),
                                             new ZKClientModule());
    configuration = injector.getInstance(CConfiguration.class);

    if (configuration.getBoolean(Constants.Security.ENABLED)) {
      this.zkClientService = injector.getInstance(ZKClientService.class);
      this.authServer = injector.getInstance(ExternalAuthenticationServer.class);
    }
  }

  @Override
  public void start() {
    if (authServer != null) {
      try {
        LOG.info("Starting AuthenticationServer.");

        // Enable Kerberos login
        SecurityUtil.enableKerberosLogin(configuration);

        Services.chainStart(zkClientService, authServer);
      } catch (Exception e) {
        LOG.error("Got exception while starting authenticaion server", e);
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
