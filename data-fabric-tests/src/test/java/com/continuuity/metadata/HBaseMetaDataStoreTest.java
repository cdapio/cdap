package com.continuuity.metadata;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.hbase.HBaseTestFactory;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
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
  private static HBaseTestBase testHBase;

  @BeforeClass
  public static void setupDataFabric() throws Exception {
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.Zookeeper.QUORUM, testHBase.getZkConnectionString());
    // tests should interact with HDFS as the current user
    conf.unset(Constants.CFG_HDFS_USER);
    conf.setBoolean(TxConstants.DataJanitor.CFG_TX_JANITOR_ENABLE, false);
    DataFabricDistributedModule dfModule = new DataFabricDistributedModule();
    Module module = Modules.override(dfModule).with(
      new AbstractModule() {
        @Override
        protected void configure() {
          // prevent going through network for transactions
          bind(TransactionSystemClient.class).to(InMemoryTxSystemClient.class);
        }
      });
    injector = Guice.createInjector(module,
                                    new ConfigModule(conf, testHBase.getConfiguration()),
                                    new ZKClientModule(),
                                    new DiscoveryRuntimeModule().getDistributedModules(),
                                    new TransactionMetricsModule(),
                                    new LocationRuntimeModule().getDistributedModules());
  }

  @AfterClass
  public static void stopHBase()
    throws Exception {
    testHBase.stopHBase();
  }
}
