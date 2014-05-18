package com.continuuity.hive.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.Networks;
import com.continuuity.hive.HiveCommandExecutor;
import com.continuuity.hive.HiveServer;
import com.continuuity.hive.distributed.DistributedHiveServer;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import com.google.inject.util.Providers;
import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 *
 */
public class DistributedHiveModule extends AbstractModule {

  private final HiveConf hiveConf;

  public DistributedHiveModule(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  @Override
  protected void configure() {
    bind(HiveConf.class).toInstance(hiveConf);
    bind(HiveServer.class).to(DistributedHiveServer.class).in(Scopes.SINGLETON);
    bind(HiveCommandExecutor.class);
  }

  @Provides
  @Named(Constants.Hive.Container.SERVER_ADDRESS)
  public final InetAddress providesHostname(CConfiguration cConf) {
    return Networks.resolve(cConf.get(Constants.Hive.Container.SERVER_ADDRESS),
                            new InetSocketAddress("localhost", 0).getAddress());
  }
}
