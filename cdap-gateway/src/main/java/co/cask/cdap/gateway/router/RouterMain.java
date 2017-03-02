/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.ServiceBindException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.runtime.DaemonMain;
import co.cask.cdap.route.store.RouteStore;
import co.cask.cdap.route.store.ZKRouteStore;
import co.cask.cdap.security.guice.SecurityModules;
import co.cask.cdap.security.impersonation.SecurityUtil;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
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
  private RouteStore routeStore;

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
        int foundPaths = RouterAuditLookUp.getAuditLookUp().getNumberOfPaths();
        if (cConf.getBoolean(Constants.Router.ROUTER_AUDIT_PATH_CHECK_ENABLED) &&
          foundPaths != ExceptedNumberOfAuditPolicyPaths.EXPECTED_PATH_NUMBER) {
          LOG.error("Failed to start the router due to the incorrect number of paths with AuditPolicy. " +
                      "Expected: {}, found: {}", ExceptedNumberOfAuditPolicyPaths.EXPECTED_PATH_NUMBER, foundPaths);
          System.exit(1);
        }
        // Enable Kerberos login
        SecurityUtil.enableKerberosLogin(cConf);
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

      // Get RouteStore so that we can close it when shutting down
      routeStore = injector.getInstance(RouteStore.class);
      LOG.info("Router initialized.");
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void start() throws Exception {
    LOG.info("Starting Router...");
    co.cask.cdap.common.service.Services.startAndWait(zkClientService,
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
    try {
      routeStore.close();
    } catch (Exception ex) {
      LOG.debug("Exception when trying to close RouteStore.", ex);
    }
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
      new DiscoveryRuntimeModule().getDistributedModules(),
      new RouterModules().getDistributedModules(),
      new SecurityModules().getDistributedModules(),
      new IOModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(RouteStore.class).to(ZKRouteStore.class).in(Scopes.SINGLETON);
        }
      }
    );
  }
}
