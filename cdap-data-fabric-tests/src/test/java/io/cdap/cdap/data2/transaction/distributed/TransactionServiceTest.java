/*
 * Copyright © 2014-2023 Cask Data, Inc.
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

package io.cdap.cdap.data2.transaction.distributed;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.guice.ZkClientModule;
import io.cdap.cdap.common.guice.ZkDiscoveryModule;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data.runtime.TransactionMetricsModule;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryTable;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.impersonation.UnsupportedUGIProvider;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.noop.NoopMetadataStorage;
import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.tephra.DefaultTransactionExecutor;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.tephra.distributed.TransactionService;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Testing HA for {@link TransactionService}.
 */
public class TransactionServiceTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();
  private Configuration hConf;
  private InMemoryZKServer zkServer;
  private MiniDFSCluster miniDFSCluster;

  @Before
  public void before() throws Exception {
    hConf = new Configuration();

    miniDFSCluster = new MiniDFSCluster.Builder(hConf).numDataNodes(1).build();
    miniDFSCluster.waitClusterUp();
    hConf.setBoolean("fs.hdfs.impl.disable.cache", true);

    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();
  }

  @After
  public void after() throws Exception {
    try {
      miniDFSCluster.shutdown();
    } finally {
      if (zkServer != null) {
        zkServer.stopAndWait();
      }
    }
  }

  @Test(timeout = 30000)
  public void testHighAvailability() throws Exception {

    // NOTE: we play with blocking/nonblocking a lot below
    //       as until we integrate with "leader election" stuff, service blocks on start if it is not a leader
    // TODO: fix this by integration with generic leader election stuff

    CConfiguration cConf = CConfiguration.create();
    // tests should use the current user for HDFS
    cConf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());

    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        RemoteAuthenticatorModules.getNoOpModule(),
        new ZkClientModule(),
        new ZkDiscoveryModule(),
        new NonCustomLocationUnitTestModule(),
        new TransactionMetricsModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(NamespaceQueryAdmin.class).to(SimpleNamespaceQueryAdmin.class);
            bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
            bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
          }
        },
        new DataFabricModules().getDistributedModules(),
        Modules.override(new DataSetsModules().getDistributedModules()).with(new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetadataStorage.class).to(NoopMetadataStorage.class);
          }
        }),
        new AuthorizationTestModule(),
        new AuthorizationEnforcementModule().getInMemoryModules(),
        new AuthenticationContextModules().getNoOpModule()
    );

    ZKClientService zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();

    try {
      final Table table = createTable("myTable");
      // tx service client
      // NOTE: we can init it earlier than we start services, it should pick them up when they are available
      TransactionSystemClient txClient = injector.getInstance(TransactionSystemClient.class);

      TransactionExecutor txExecutor = new DefaultTransactionExecutor(txClient,
          ImmutableList.of((TransactionAware) table));

      // starting tx service, tx client can pick it up
      TransactionService first = createTxService(zkServer.getConnectionStr(),
          Networks.getRandomPort(),
          hConf, tmpFolder.newFolder());
      first.startAndWait();
      Assert.assertNotNull(txClient.startShort());
      verifyGetAndPut(table, txExecutor, null, "val1");

      // starting another tx service should not hurt
      TransactionService second = createTxService(zkServer.getConnectionStr(),
          Networks.getRandomPort(),
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
      TransactionService third = createTxService(zkServer.getConnectionStr(),
          Networks.getRandomPort(),
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
      try {
        dropTable("myTable");
      } finally {
        zkClient.stopAndWait();
      }
    }
  }

  private void verifyGetAndPut(final Table table, TransactionExecutor txExecutor,
      final String verifyGet, final String toPut)
      throws TransactionFailureException, InterruptedException {

    txExecutor.execute(() -> {
      byte[] existing = table.get(Bytes.toBytes("row"), Bytes.toBytes("col"));
      Assert.assertTrue((verifyGet == null && existing == null)
          || Arrays.equals(Bytes.toBytes(verifyGet), existing));
      table.put(Bytes.toBytes("row"), Bytes.toBytes("col"), Bytes.toBytes(toPut));
    });
  }

  private Table createTable(String tableName) {
    InMemoryTableService.create(tableName);
    return new InMemoryTable(tableName);
  }

  private void dropTable(String tableName) {
    InMemoryTableService.drop(tableName);
  }

  private static TransactionService createTxService(String zkConnectionString, int txServicePort,
      Configuration hConf, final File outPath) {
    return createTxService(zkConnectionString, txServicePort, hConf, outPath, null);
  }

  static TransactionService createTxService(String zkConnectionString, int txServicePort,
      Configuration hConf, final File outPath,
      @Nullable CConfiguration cConfig) {
    final CConfiguration cConf = cConfig == null ? CConfiguration.create() : cConfig;
    // tests should use the current user for HDFS
    cConf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));
    cConf.set(Constants.Zookeeper.QUORUM, zkConnectionString);
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, outPath.getAbsolutePath());
    cConf.set(TxConstants.Service.CFG_DATA_TX_BIND_PORT,
        Integer.toString(txServicePort));
    // we want persisting for this test
    cConf.setBoolean(TxConstants.Manager.CFG_DO_PERSIST, true);
    cConf.setBoolean(TxConstants.TransactionPruning.PRUNE_ENABLE, false);

    final Injector injector =
        Guice.createInjector(new ConfigModule(cConf, hConf),
            new NonCustomLocationUnitTestModule(),
            new ZkClientModule(),
            new ZkDiscoveryModule(),
            new TransactionMetricsModule(),
            new AbstractModule() {
              @Override
              protected void configure() {
                bind(NamespaceQueryAdmin.class).to(SimpleNamespaceQueryAdmin.class);
                bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
                bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
              }
            },
            new DataFabricModules().getDistributedModules(),
            new SystemDatasetRuntimeModule().getInMemoryModules(),
            new DataSetsModules().getInMemoryModules(),
            new AuthorizationTestModule(),
            new AuthorizationEnforcementModule().getInMemoryModules(),
            new AuthenticationContextModules().getNoOpModule());
    injector.getInstance(ZKClientService.class).startAndWait();

    return injector.getInstance(TransactionService.class);
  }
}
