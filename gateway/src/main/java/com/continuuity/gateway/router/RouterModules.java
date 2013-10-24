package com.continuuity.gateway.router;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.filesystem.LocationFactories;
import com.continuuity.weave.filesystem.LocationFactory;
import com.continuuity.weave.yarn.YarnWeaveRunnerService;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.util.Modules;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Router guice modules.
 */
public class RouterModules extends RuntimeModule {
  @Override
  public Module getInMemoryModules() {
    return getCommonModules();
  }

  @Override
  public Module getSingleNodeModules() {
    return getCommonModules();
  }

  @Override
  public Module getDistributedModules() {
    return Modules.combine(getCommonModules(), new PrivateModule() {
      @Override
      protected void configure() {
        bind(WeaveRunnerService.class).to(YarnWeaveRunnerService.class);
        bind(new TypeLiteral<Iterable<WeaveRunner.LiveInfo>>() {}).toProvider(WeaveLiveInfoProvider.class);
        expose(WeaveRunnerService.class);
      }

      @Singleton
      @Provides
      private YarnWeaveRunnerService provideYarnWeaveRunnerService(CConfiguration configuration,
                                                                   YarnConfiguration yarnConfiguration,
                                                                   LocationFactory locationFactory) {
        String zkNamespace = configuration.get(Constants.CFG_WEAVE_ZK_NAMESPACE, "/weave");
        YarnConfiguration yarnConfig = new YarnConfiguration(yarnConfiguration);
        yarnConfig.set(Constants.CFG_WEAVE_RESERVED_MEMORY_MB,
                       configuration.get(Constants.CFG_WEAVE_RESERVED_MEMORY_MB));
        return new YarnWeaveRunnerService(yarnConfig,
                                          configuration.get(Constants.Zookeeper.QUORUM) + zkNamespace,
                                          LocationFactories.namespace(locationFactory, "weave"));
      }
    });
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

  private Module getCommonModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
      }

      @Provides
      @Named(Constants.Router.ADDRESS)
      public final InetAddress providesHostname(CConfiguration cConf) {
        return Networks.resolve(cConf.get(Constants.Router.ADDRESS),
                                new InetSocketAddress("localhost", 0).getAddress());
      }
    };
  }
}
