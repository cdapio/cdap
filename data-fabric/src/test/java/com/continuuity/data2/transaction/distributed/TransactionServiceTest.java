/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.distributed;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.InMemoryDataSetAccessor;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.continuuity.weave.filesystem.LocationFactory;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * HBase queue tests.
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
      cConf.unset(Constants.CFG_HDFS_USER);
      cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());

      final OrderedColumnarTable table = createTable("myTable", cConf);
      try {
        // tx service client
        // NOTE: we can init it earlier than we start services, it should pick them up when they are available
        TransactionSystemClient txClient = new TransactionServiceClient(cConf);

        TransactionExecutor txExecutor = new DefaultTransactionExecutor(txClient,
                                                                        ImmutableList.of((TransactionAware) table));

        // starting tx service, tx client can pick it up
        TransactionService first = createTxService(zkServer.getConnectionStr(), Networks.getRandomPort(), hConf);
        first.startAndWait();
        Assert.assertNotNull(txClient.startShort());
        verifyGetAndPut(table, txExecutor, null, "val1");

        // starting another tx service should not hurt
        TransactionService second = createTxService(zkServer.getConnectionStr(), Networks.getRandomPort(), hConf);
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
        TransactionService third = createTxService(zkServer.getConnectionStr(), Networks.getRandomPort(), hConf);
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
      }

    } finally {
      zkServer.stop();
    }
  }

  private void verifyGetAndPut(final OrderedColumnarTable table, TransactionExecutor txExecutor,
                               final String verifyGet, final String toPut) throws TransactionFailureException {
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

  private OrderedColumnarTable createTable(String tableName, CConfiguration cConf) throws Exception {
    DataSetAccessor dsAccessor = new InMemoryDataSetAccessor(cConf);
    DataSetManager dsManager =
      dsAccessor.getDataSetManager(OrderedColumnarTable.class, DataSetAccessor.Namespace.USER);
    dsManager.create(tableName);
    return dsAccessor.getDataSetClient(tableName, OrderedColumnarTable.class, DataSetAccessor.Namespace.USER);
  }

  private void dropTable(String tableName, CConfiguration cConf) throws Exception {
    DataSetAccessor dsAccessor = new InMemoryDataSetAccessor(cConf);
    DataSetManager dsManager =
      dsAccessor.getDataSetManager(OrderedColumnarTable.class, DataSetAccessor.Namespace.USER);
    dsManager.drop(tableName);
  }

  private TransactionService createTxService(String zkConnectionString, int txServicePort, Configuration hConf) {
    final CConfiguration cConf = CConfiguration.create();
    // tests should use the current user for HDFS
    cConf.unset(Constants.CFG_HDFS_USER);
    cConf.set(Constants.Zookeeper.QUORUM, zkConnectionString);
    cConf.set(Constants.Transaction.Service.CFG_DATA_TX_BIND_PORT,
              Integer.toString(txServicePort));
    // we want persisting for this test
    cConf.setBoolean(Constants.Transaction.Manager.CFG_DO_PERSIST, true);

    final DataFabricDistributedModule dfModule = new DataFabricDistributedModule(cConf, hConf);

    ZKClientService zkClientService = getZkClientService(cConf);
    zkClientService.start();

    final Injector injector =
      Guice.createInjector(dfModule,
                           new DiscoveryRuntimeModule(zkClientService).getDistributedModules(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               try {
                                 bind(LocationFactory.class)
                                   .toInstance(new LocalLocationFactory(tmpFolder.newFolder()));
                               } catch (IOException e) {
                                 throw Throwables.propagate(e);
                               }
                             }
                           });

    return injector.getInstance(TransactionService.class);
  }

  private static ZKClientService getZkClientService(CConfiguration cConf) {
    return ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(
          ZKClientService.Builder.of(cConf.get(Constants.Zookeeper.QUORUM))
                                .setSessionTimeout(cConf.getInt(Constants.Zookeeper.CFG_SESSION_TIMEOUT_MILLIS,
                                                                Constants.Zookeeper.DEFAULT_SESSION_TIMEOUT_MILLIS))
                                .build(),
                                RetryStrategies.fixDelay(2, TimeUnit.SECONDS)
        )
      )
    );
  }

}
