package com.continuuity.hive.inmemory;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.persist.NoOpTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.continuuity.hive.HiveCommandExecutor;
import com.continuuity.hive.HiveServer;
import com.continuuity.hive.HiveServerTest;
import com.continuuity.hive.guice.InMemoryHiveModule;
import com.google.inject.AbstractModule;
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

    Injector injector = Guice.createInjector(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(TransactionStateStorage.class).to(NoOpTransactionStateStorage.class);
            bind(InMemoryTransactionManager.class);
          }
        },
        new ConfigModule(conf, hConf),
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
