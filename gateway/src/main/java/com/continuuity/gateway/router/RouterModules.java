package com.continuuity.gateway.router;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.common.utils.Networks;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Named;

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
    return getCommonModules();
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
