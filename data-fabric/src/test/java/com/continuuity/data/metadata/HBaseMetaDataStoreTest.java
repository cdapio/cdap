package com.continuuity.data.metadata;

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
public abstract class HBaseMetaDataStoreTest extends MetaDataStoreTest {

  protected static Injector injector;

  @BeforeClass
  public static void setupDataFabric() throws Exception {
    HBaseTestBase.startHBase();
    DataFabricDistributedModule dfModule = new DataFabricDistributedModule(HBaseTestBase.getConfiguration());
    Module module = Modules.override(dfModule).with(
      new AbstractModule() {
        @Override
        protected void configure() {
          // prevent going through network for transactions
          bind(TransactionSystemClient.class).to(InMemoryTxSystemClient.class);
        }
      });
    dfModule.getConfiguration().set(Constants.Zookeeper.QUORUM, HBaseTestBase.getZkConnectionString());
    // tests should interact with HDFS as the current user
    dfModule.getConfiguration().unset(Constants.CFG_HDFS_USER);
    injector = Guice.createInjector(module,
                                    new ConfigModule(dfModule.getConfiguration(),
                                                     HBaseTestBase.getConfiguration()),
                                    new LocationRuntimeModule().getInMemoryModules());
  }

  @AfterClass
  public static void stopHBase()
    throws Exception {
    HBaseTestBase.stopHBase();
  }
}
