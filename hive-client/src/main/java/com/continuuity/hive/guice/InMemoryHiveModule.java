package com.continuuity.hive.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.Networks;
import com.continuuity.hive.HiveCommandExecutor;
import com.continuuity.hive.HiveServer;
import com.continuuity.hive.inmemory.LocalHiveServer;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 *
 */
public class InMemoryHiveModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(HiveServer.class).to(LocalHiveServer.class).in(Scopes.SINGLETON);
    bind(HiveCommandExecutor.class);
  }

  @Provides
  @Named(Constants.Hive.SERVER_ADDRESS)
  public final InetAddress providesHostname(CConfiguration cConf) {
    return Networks.resolve(cConf.get(Constants.Hive.SERVER_ADDRESS),
                            new InetSocketAddress("localhost", 0).getAddress());
  }
}
