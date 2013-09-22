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
import com.continuuity.data2.transaction.inmemory.StatePersistor;
import com.continuuity.data2.transaction.inmemory.ZooKeeperPersistor;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.continuuity.weave.filesystem.LocationFactory;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * HBase queue tests.
 */
public class TransactionServiceTest {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionServiceTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testHA() throws Exception {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    // NOTE: we play with blocking/nonblocking a lot below
    //       as until we integrate with "leader election" stuff, service blocks on start if it is not a leader
    // TODO: fix this by integration with generic leader election stuff

    try {
      CConfiguration cConf = new CConfiguration();
      cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
      final OrderedColumnarTable table = createTable("myTable", cConf);
      try {
        // tx service client
        // NOTE: we can init it earlier than we start services, it should pick them up when they are available
        TransactionSystemClient txClient = new TransactionServiceClient(cConf);

        TransactionExecutor txExecutor = new DefaultTransactionExecutor(txClient,
                                                                        ImmutableList.of((TransactionAware) table));

        // starting tx service, tx client can pick it up
        TransactionService first = createTxService(zkServer.getConnectionStr(), Networks.getRandomPort());
        first.startAndWait();
        Assert.assertNotNull(txClient.startShort());
        verifyGetAndPut(table, txExecutor, null, "val1");

        // starting another tx service should not hurt
        TransactionService second = createTxService(zkServer.getConnectionStr(), Networks.getRandomPort());
        ListenableFuture<Service.State> secondStarted = second.start();
        // wait for affect a bit
        TimeUnit.SECONDS.sleep(1);

        Assert.assertNotNull(txClient.startShort());
        verifyGetAndPut(table, txExecutor, "val1", "val2");

        // shutting down the first one is fine: we have another one to pick up the leader role
        first.stopAndWait();
        // waiting for second to start, todo: we don't have to: client should wait
//        secondStarted.get();

        Assert.assertNotNull(txClient.startShort());
        verifyGetAndPut(table, txExecutor, "val2", "val3");

        // doing same trick again to failover to the third one
        TransactionService third = createTxService(zkServer.getConnectionStr(), Networks.getRandomPort());
        ListenableFuture<Service.State> thirdStarted = third.start();
        // stopping second one
        second.stopAndWait();
        // waiting for third to come up, todo: we don't have to: client should wait
//        thirdStarted.get();
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

  private void verifyGetAndPut(final OrderedColumnarTable table, TransactionExecutor txExecutor, final String verifyGet, final String toPut) throws TransactionFailureException {
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

  private TransactionService createTxService(String zkConnectionString, int txServicePort) {
    final CConfiguration cConf = new CConfiguration();
    cConf.set(Constants.Zookeeper.QUORUM, zkConnectionString);
    cConf.set(com.continuuity.data2.transaction.distributed.Constants.CFG_DATA_TX_SERVER_PORT,
              Integer.toString(txServicePort));
    // we want persisting for this test
    cConf.setBoolean(StatePersistor.CFG_DO_PERSIST, true);

    final DataFabricDistributedModule dfModule =
      new DataFabricDistributedModule(cConf);
    // configuring persistence
    final Module dataFabricModule = Modules.override(dfModule).with(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(StatePersistor.class).toInstance(new ZooKeeperPersistor(cConf));
        }
      });

    ZKClientService zkClientService = getZkClientService(zkConnectionString);
    zkClientService.start();

    final Injector injector =
      Guice.createInjector(dataFabricModule,
                           new DiscoveryRuntimeModule(zkClientService).getDistributedModules(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               try {
                                 bind(LocationFactory.class).toInstance(new LocalLocationFactory(tmpFolder.newFolder()));
                               } catch (IOException e) {
                                 throw Throwables.propagate(e);
                               }
                             }
                           });

    return injector.getInstance(TransactionService.class);
  }

  private static ZKClientService getZkClientService(String zkConnectionString) {
    return ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(
          ZKClientService.Builder.of(zkConnectionString).setSessionTimeout(10000).build(),
          RetryStrategies.fixDelay(2, TimeUnit.SECONDS)
        )
      )
    );
  }

}
