package com.continuuity.hive.inmemory;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.hive.HiveCommandExecutor;
import com.continuuity.hive.HiveServer;
import com.continuuity.hive.HiveServerTest;
import com.continuuity.hive.guice.InMemoryHiveModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;

/**
 *
 */
public class LocalHiveServerTest extends HiveServerTest {

  private HiveServer hiveServer;
  private HiveCommandExecutor hiveCommandExecutor;

  public LocalHiveServerTest() {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.Hive.SERVER_ADDRESS, "localhost");
    Configuration hConf = new Configuration();
//    hConf.addResource("mapred-site-local.xml");
//    hConf.reloadConfiguration();

    Injector injector = Guice.createInjector(new ConfigModule(conf, hConf),
        new InMemoryHiveModule(),
        new DiscoveryRuntimeModule().getInMemoryModules());
    hiveServer = injector.getInstance(HiveServer.class);
    hiveCommandExecutor = injector.getInstance(HiveCommandExecutor.class);
  }

  @Override
  protected HiveServer getHiveServer() {
    return hiveServer;
  }

  @Override
  protected HiveCommandExecutor getHiveCommandExecutor() {
    return hiveCommandExecutor;
  }
}
