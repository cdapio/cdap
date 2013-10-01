package com.continuuity.gateway.router;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.common.utils.Networks;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.common.Services;
import com.continuuity.weave.filesystem.LocationFactories;
import com.continuuity.weave.filesystem.LocationFactory;
import com.continuuity.weave.yarn.YarnWeaveRunnerService;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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

  private ZKClientService zkClientService;
  private WeaveRunnerService weaveRunnerService;
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

      weaveRunnerService = injector.getInstance(WeaveRunnerService.class);

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
    Futures.getUnchecked(Services.chainStart(zkClientService, weaveRunnerService, router));
    LOG.info("Router started.");
  }

  @Override
  public void stop() {
    LOG.info("Stopping Router...");
    Futures.getUnchecked(Services.chainStop(router, weaveRunnerService, zkClientService));
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
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(WeaveRunnerService.class).to(YarnWeaveRunnerService.class);
          bind(new TypeLiteral<Iterable<WeaveRunner.LiveInfo>>() {}).toProvider(WeaveLiveInfoProvider.class);
        }

        @Provides
        @Named(Constants.Router.ADDRESS)
        public final InetAddress providesHostname(CConfiguration cConf) {
          return Networks.resolve(cConf.get(Constants.Router.ADDRESS),
                                  new InetSocketAddress("localhost", 0).getAddress());
        }

        @Singleton
        @Provides
        private YarnWeaveRunnerService provideYarnWeaveRunnerService(CConfiguration configuration,
                                                                     YarnConfiguration yarnConfiguration,
                                                                     LocationFactory locationFactory) {
          String zkNamespace = configuration.get(Constants.CFG_WEAVE_ZK_NAMESPACE, "/weave");
          return new YarnWeaveRunnerService(yarnConfiguration,
                                            configuration.get(Constants.Zookeeper.QUORUM) + zkNamespace,
                                            LocationFactories.namespace(locationFactory, "weave"));
        }
      }
    );
  }

  private static class WeaveLiveInfoProvider implements Provider<Iterable<WeaveRunner.LiveInfo>> {
    private final WeaveRunnerService weaveRunnerService;

    @Inject
    private WeaveLiveInfoProvider(WeaveRunnerService weaveRunnerService) {
      this.weaveRunnerService = weaveRunnerService;
    }

    @Override
    public Iterable<WeaveRunner.LiveInfo> get() {
      return weaveRunnerService.lookupLive();
    }
  }
}
