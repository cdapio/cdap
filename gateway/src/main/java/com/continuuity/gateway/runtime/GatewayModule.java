package com.continuuity.gateway.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.gateway.Gateway;
import com.continuuity.gateway.collector.NettyFlumeCollector;
import com.continuuity.gateway.handlers.GatewayCommonHandlerModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Guice modules for Gateway.
 */
public class GatewayModule extends RuntimeModule {

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
    return getCommonModules();
  }

  private Module getCommonModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        install(new GatewayCommonHandlerModule());

        bind(Gateway.class);
        expose(Gateway.class);

        bind(NettyFlumeCollector.class);
        expose(NettyFlumeCollector.class);
      }

      @Provides
      @Named(Constants.Gateway.ADDRESS)
      public final InetAddress providesHostname(CConfiguration cConf) {
        return Networks.resolve(cConf.get(Constants.Gateway.ADDRESS),
                                new InetSocketAddress("localhost", 0).getAddress());
      }
    };
  }
}
