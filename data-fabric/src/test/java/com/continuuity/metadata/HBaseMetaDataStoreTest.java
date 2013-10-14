package com.continuuity.metadata;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * HBase meta data store tests.
 */
public abstract class HBaseMetaDataStoreTest extends MetaDataTableTest {

  protected static Injector injector;

  @BeforeClass
  public static void setupDataFabric() throws Exception {
    HBaseTestBase.startHBase();
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.Zookeeper.QUORUM, HBaseTestBase.getZkConnectionString());
    // tests should interact with HDFS as the current user
    conf.unset(Constants.CFG_HDFS_USER);
    conf.setBoolean(Constants.Transaction.DataJanitor.CFG_TX_JANITOR_ENABLE, false);
    DataFabricDistributedModule dfModule = new DataFabricDistributedModule(conf, HBaseTestBase.getConfiguration());
    Module module = Modules.override(dfModule).with(
      new AbstractModule() {
        @Override
        protected void configure() {
          // prevent going through network for transactions
          bind(TransactionSystemClient.class).to(InMemoryTxSystemClient.class);
        }
      });
    injector = Guice.createInjector(module,
                                    new ConfigModule(conf, HBaseTestBase.getConfiguration()),
                                    new LocationRuntimeModule().getDistributedModules());
  }

  @AfterClass
  public static void stopHBase()
    throws Exception {
    HBaseTestBase.stopHBase();
  }
}
