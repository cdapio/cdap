/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.data2.transaction.distributed;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.data.runtime.DataFabricDistributedModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryTable;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.TxConstants;
import co.cask.tephra.distributed.TransactionService;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Testing HA for {@link TransactionService}.
 */
public class TransactionServiceTest {
  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test(timeout = 60000)
  public void testHA() throws Exception {
    HBaseTestingUtility hBaseTestingUtility = new HBaseTestingUtility();
    hBaseTestingUtility.startMiniDFSCluster(1);
    Configuration hConf = hBaseTestingUtility.getConfiguration();
    hConf.setBoolean("fs.hdfs.impl.disable.cache", true);

    InMemoryZKServer zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    // NOTE: we play with blocking/nonblocking a lot below
    //       as until we integrate with "leader election" stuff, service blocks on start if it is not a leader
    // TODO: fix this by integration with generic leader election stuff

    try {
      CConfiguration cConf = CConfiguration.create();
      // tests should use the current user for HDFS
      cConf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));
      cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
      cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());

      Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        new ZKClientModule(),
        new LocationRuntimeModule().getInMemoryModules(),
        new DiscoveryRuntimeModule().getDistributedModules(),
        new TransactionMetricsModule(),
        new DataFabricModules().getDistributedModules(),
        new DataSetsModules().getDistributedModule()
      );

      ZKClientService zkClient = injector.getInstance(ZKClientService.class);
      zkClient.startAndWait();

      final Table table = createTable("myTable");
      try {
        // tx service client
        // NOTE: we can init it earlier than we start services, it should pick them up when they are available
        TransactionSystemClient txClient = injector.getInstance(TransactionSystemClient.class);

        TransactionExecutor txExecutor = new DefaultTransactionExecutor(txClient,
                                                                        ImmutableList.of((TransactionAware) table));

        // starting tx service, tx client can pick it up
        TransactionService first = createTxService(zkServer.getConnectionStr(), Networks.getRandomPort(),
                                                   hConf, tmpFolder.newFolder());
        first.startAndWait();
        Assert.assertNotNull(txClient.startShort());
        verifyGetAndPut(table, txExecutor, null, "val1");

        // starting another tx service should not hurt
        TransactionService second = createTxService(zkServer.getConnectionStr(), Networks.getRandomPort(),
                                                    hConf, tmpFolder.newFolder());
        // NOTE: we don't have to wait for start as client should pick it up anyways, but we do wait to ensure
        //       the case with two active is handled well
        second.startAndWait();
        // wait for affect a bit
        TimeUnit.SECONDS.sleep(1);

        Assert.assertNotNull(txClient.startShort());
        verifyGetAndPut(table, txExecutor, "val1", "val2");

        // shutting down the first one is fine: we have another one to pick up the leader role
        first.stopAndWait();

        Assert.assertNotNull(txClient.startShort());
        verifyGetAndPut(table, txExecutor, "val2", "val3");

        // doing same trick again to failover to the third one
        TransactionService third = createTxService(zkServer.getConnectionStr(), Networks.getRandomPort(),
                                                   hConf, tmpFolder.newFolder());
        // NOTE: we don't have to wait for start as client should pick it up anyways
        third.start();
        // stopping second one
        second.stopAndWait();

        Assert.assertNotNull(txClient.startShort());
        verifyGetAndPut(table, txExecutor, "val3", "val4");

        // releasing resources
        third.stop();
      } finally {
        dropTable("myTable", cConf);
        zkClient.stopAndWait();
      }

    } finally {
      zkServer.stop();
    }
  }

  private void verifyGetAndPut(final Table table, TransactionExecutor txExecutor,
                               final String verifyGet, final String toPut)
    throws TransactionFailureException, InterruptedException {

    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        byte[] existing = table.get(Bytes.toBytes("row"), Bytes.toBytes("col"));
        Assert.assertTrue((verifyGet == null && existing == null) ||
                            Arrays.equals(Bytes.toBytes(verifyGet), existing));
        table.put(Bytes.toBytes("row"), Bytes.toBytes("col"), Bytes.toBytes(toPut));
      }
    });
  }

  private Table createTable(String tableName) throws Exception {
    InMemoryTableService.create(tableName);
    return new InMemoryTable(tableName);
  }

  private void dropTable(String tableName, CConfiguration cConf) throws Exception {
    InMemoryTableService.drop(tableName);
  }

  static TransactionService createTxService(String zkConnectionString, int txServicePort,
                                             Configuration hConf, final File outPath) {
    final CConfiguration cConf = CConfiguration.create();
    // tests should use the current user for HDFS
    cConf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));
    cConf.set(Constants.Zookeeper.QUORUM, zkConnectionString);
    cConf.set(TxConstants.Service.CFG_DATA_TX_BIND_PORT,
              Integer.toString(txServicePort));
    // we want persisting for this test
    cConf.setBoolean(TxConstants.Manager.CFG_DO_PERSIST, true);

    final DataFabricDistributedModule dfModule = new DataFabricDistributedModule();

    final Injector injector =
      Guice.createInjector(dfModule,
                           new ConfigModule(cConf, hConf),
                           new ZKClientModule(),
                           new DiscoveryRuntimeModule().getDistributedModules(),
                           new TransactionMetricsModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(LocationFactory.class)
                                 .toInstance(new LocalLocationFactory(outPath));
                             }
                           });
    injector.getInstance(ZKClientService.class).startAndWait();

    return injector.getInstance(TransactionService.class);
  }
}
