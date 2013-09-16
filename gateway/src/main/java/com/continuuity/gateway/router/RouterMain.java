package com.continuuity.gateway.router;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.common.utils.Networks;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
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
      LOG.error("Got exception", e);
    }
  }

  @Override
  public void init(String[] args) {
    LOG.info("Initializing Router...");
    try {
      // Load configuration
      cConf = CConfiguration.create();

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

      Injector injector = createGuiceInjector();

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
    zkClientService.startAndWait();
    router.startAndWait();
    LOG.info("Router started.");
  }

  @Override
  public void stop() {
    LOG.info("Stopping Router...");
    router.stopAndWait();
    zkClientService.stopAndWait();
    LOG.info("Router stopped.");
  }

  @Override
  public void destroy() {
    // Nothing to do
  }

  private Injector createGuiceInjector() {
    return Guice.createInjector(
      new ConfigModule(cConf),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule(zkClientService).getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
        }

        @Provides
        @Named(Constants.Router.ADDRESS)
        public final InetAddress providesHostname(CConfiguration cConf) {
          return Networks.resolve(cConf.get(Constants.Router.ADDRESS),
                                  new InetSocketAddress("localhost", 0).getAddress());
        }
      }
    );
  }
}
