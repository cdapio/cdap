package com.continuuity.hive.inmemory;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricInMemoryModule;
import com.continuuity.data.runtime.DataSetServiceModules;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.hive.HiveServerTest;
import com.continuuity.hive.client.HiveClient;
import com.continuuity.hive.client.guice.HiveClientModule;
import com.continuuity.hive.guice.HiveRuntimeModule;
import com.continuuity.hive.server.HiveServer;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;

/**
 *
 */
public class LocalHiveServerTest extends HiveServerTest {

  private final HiveServer hiveServer;
  private final HiveClient hiveClient;
  private final InMemoryHiveMetastore hiveMetastore;
  private final InMemoryTransactionManager transactionManager;

  public LocalHiveServerTest() {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.Hive.SERVER_ADDRESS, "localhost");
    Configuration hConf = new Configuration();

    Injector injector = Guice.createInjector(
        new DataFabricInMemoryModule(conf),
        new LocationRuntimeModule().getInMemoryModules(),
        new ConfigModule(conf, hConf),
        new HiveRuntimeModule().getInMemoryModules(),
        new DiscoveryRuntimeModule().getInMemoryModules(),
        new MetricsClientRuntimeModule().getInMemoryModules(),
        new DataSetServiceModules().getInMemoryModule(),
        new AuthModule(),
        new HiveClientModule());
    hiveServer = injector.getInstance(HiveServer.class);
    hiveMetastore = injector.getInstance(InMemoryHiveMetastore.class);
    hiveClient = injector.getInstance(HiveClient.class);
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
  }

  @Override
  protected HiveClient getHiveClient() {
    return hiveClient;
  }

  @Override
  protected void startServices() {
    hiveMetastore.startAndWait();
    hiveServer.startAndWait();
    transactionManager.startAndWait();
  }

  @Override
  protected void stopServices() {
    if (hiveServer != null) {
      hiveServer.stopAndWait();
    }
    if (hiveMetastore != null) {
      hiveMetastore.stopAndWait();
    }
  }
}
