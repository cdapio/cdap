package com.continuuity.hive.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.hive.HiveCommandExecutor;
import com.continuuity.hive.HiveServer;
import com.continuuity.hive.distributed.DistributedHiveServer;
import com.continuuity.hive.inmemory.LocalHiveServer;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import com.google.inject.util.Modules;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Hive Runtime guice module.
 */
public class HiveRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return Modules.combine(new HiveModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(HiveServer.class).to(LocalHiveServer.class).in(Scopes.SINGLETON);
                             }
                           });
  }

  @Override
  public Module getSingleNodeModules() {
    return getInMemoryModules();
  }

  @Override
  public Module getDistributedModules() {
    return Modules.combine(new HiveModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(HiveServer.class).to(DistributedHiveServer.class).in(Scopes.SINGLETON);
          }
        });
  }

  private static final class HiveModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(HiveCommandExecutor.class);
    }

    @Provides
    @Named(Constants.Hive.Container.SERVER_ADDRESS)
    public final InetAddress providesHostname(CConfiguration cConf) {
      return Networks.resolve(cConf.get(Constants.Hive.Container.SERVER_ADDRESS),
          new InetSocketAddress("localhost", 0).getAddress());
    }
  }
}
