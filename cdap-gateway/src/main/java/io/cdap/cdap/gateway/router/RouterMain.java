/*
 * Copyright © 2014-2021 Cask Data, Inc.
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

package io.cdap.cdap.gateway.router;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.ServiceBindException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
import io.cdap.cdap.common.runtime.DaemonMain;
import io.cdap.cdap.security.guice.SecurityModules;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import org.apache.twill.internal.Services;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Main class to run Router from command line.
 */
public class RouterMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(RouterMain.class);

  private CConfiguration cConf;
  private ZKClientService zkClientService;
  private NettyRouter router;

  public static void main(String[] args) {
    try {
      new RouterMain().doMain(args);
    } catch (Throwable e) {
      Throwable rootCause = Throwables.getRootCause(e);
      if (rootCause instanceof ServiceBindException) {
        LOG.error("Failed to start Router: {}", rootCause.getMessage());
      } else {
        LOG.error("Failed to start Router", e);
      }
    }
  }

  @Override
  public void init(String[] args) {
    LOG.info("Initializing Router...");
    try {
      // Load configuration
      cConf = CConfiguration.create();

      if (cConf.getBoolean(Constants.Security.ENABLED)) {
        int foundPaths = RouterAuditLookUp.getInstance().getNumberOfPaths();
        if (cConf.getBoolean(Constants.Router.ROUTER_AUDIT_PATH_CHECK_ENABLED) &&
          foundPaths != ExpectedNumberOfAuditPolicyPaths.EXPECTED_PATH_NUMBER) {
          LOG.error("Failed to start the router due to the incorrect number of paths with AuditPolicy. " +
                      "Expected: {}, found: {}", ExpectedNumberOfAuditPolicyPaths.EXPECTED_PATH_NUMBER, foundPaths);
          System.exit(1);
        }
        // Enable Kerberos login
        if (SecurityUtil.isKerberosEnabled(cConf)) {
          SecurityUtil.enableKerberosLogin(cConf);
        }
      }

      // Initialize ZK client
      String zookeeper = cConf.get(Constants.Zookeeper.QUORUM);
      if (zookeeper == null) {
        LOG.error("No ZooKeeper quorum provided.");
        System.exit(1);
      }

      Injector injector = createGuiceInjector(cConf);
      zkClientService = injector.getInstance(ZKClientService.class);

      // Get the Router
      router = injector.getInstance(NettyRouter.class);

      LOG.info("Router initialized.");
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void start() throws Exception {
    LOG.info("Starting Router...");
    io.cdap.cdap.common.service.Services.startAndWait(zkClientService,
                                                      cConf.getLong(Constants.Zookeeper.CLIENT_STARTUP_TIMEOUT_MILLIS),
                                                      TimeUnit.MILLISECONDS,
                                                      String.format("Connection timed out while trying to start " +
                                                                    "ZooKeeper client. Please verify that the " +
                                                                    "ZooKeeper quorum settings are correct in " +
                                                                    "cdap-site.xml. Currently configured as: %s",
                                                                    cConf.get(Constants.Zookeeper.QUORUM)));
    router.startAndWait();
    LOG.info("Router started.");
  }

  @Override
  public void stop() {
    LOG.info("Stopping Router...");
    Futures.getUnchecked(Services.chainStop(router, zkClientService));
    LOG.info("Router stopped.");
  }

  @Override
  public void destroy() {
    // Nothing to do
  }

  static Injector createGuiceInjector(CConfiguration cConf) {
    return Guice.createInjector(
      new ConfigModule(cConf),
      new ZKClientModule(),
      new ZKDiscoveryModule(),
      new RouterModules().getDistributedModules(),
      new SecurityModules().getDistributedModules(),
      new IOModule()
    );
  }
}
