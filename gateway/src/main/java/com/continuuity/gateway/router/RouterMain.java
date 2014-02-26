package com.continuuity.gateway.router;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.runtime.DaemonMain;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.common.Services;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Main class to run Router from command line.
 */
public class RouterMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(RouterMain.class);

  private ZKClientService zkClientService;
  private TwillRunnerService twillRunnerService;
  private NettyRouter router;

  public static void main(String[] args) {
    try {
      new RouterMain().doMain(args);
    } catch (Throwable e) {
      LOG.error("Got exception", e);
    }
  }

  @Override
  public void init(String[] args) {
    LOG.info("Initializing Router...");
    try {
      // Load configuration
      CConfiguration cConf = CConfiguration.create();

      // Initialize ZK client
      String zookeeper = cConf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE);
      if (zookeeper == null) {
        LOG.error("No zookeeper quorum provided.");
        System.exit(1);
      }

      zkClientService =
        ZKClientServices.delegate(
          ZKClients.reWatchOnExpire(
            ZKClients.retryOnFailure(
              ZKClientService.Builder.of(zookeeper).build(),
              RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
            )
          ));

      Injector injector = createGuiceInjector(cConf, zkClientService);

      twillRunnerService = injector.getInstance(TwillRunnerService.class);

      // Get the Router
      router = injector.getInstance(NettyRouter.class);

      LOG.info("Router initialized.");
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void start() {
    LOG.info("Starting Router...");
    Futures.getUnchecked(Services.chainStart(zkClientService, twillRunnerService, router));
    LOG.info("Router started.");
  }

  @Override
  public void stop() {
    LOG.info("Stopping Router...");
    Futures.getUnchecked(Services.chainStop(router, twillRunnerService, zkClientService));
    LOG.info("Router stopped.");
  }

  @Override
  public void destroy() {
    // Nothing to do
  }

  static Injector createGuiceInjector(CConfiguration cConf, ZKClientService zkClientService) {
    return Guice.createInjector(
      new ConfigModule(cConf),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule(zkClientService).getDistributedModules(),
      new RouterModules().getDistributedModules()
    );
  }
}
