/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import co.cask.cdap.common.security.UGIProvider;
import co.cask.cdap.common.security.UnsupportedUGIProvider;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.metadata.store.NoOpMetadataStore;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TransactionSystemTest;
import org.apache.tephra.TxConstants;
import org.apache.tephra.distributed.TransactionService;
import org.apache.tephra.persist.TransactionSnapshot;
import org.apache.tephra.persist.TransactionStateStorage;
import org.apache.tephra.snapshot.SnapshotCodecProvider;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.InputStream;

/**
 * HBase queue tests.
 */
public class TransactionServiceClientTest extends TransactionSystemTest {
  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static InMemoryZKServer zkServer;
  private static TransactionService server;
  private static TransactionStateStorage txStateStorage;
  private static ZKClientService zkClient;
  private static Injector injector;

  @Override
  protected TransactionSystemClient getClient() throws Exception {
    return injector.getInstance(TransactionSystemClient.class);
  }

  @Override
  protected TransactionStateStorage getStateStorage() throws Exception {
    return txStateStorage;
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    HBaseTestingUtility hBaseTestingUtility = new HBaseTestingUtility();
    hBaseTestingUtility.startMiniDFSCluster(1);
    Configuration hConf = hBaseTestingUtility.getConfiguration();
    hConf.setBoolean("fs.hdfs.impl.disable.cache", true);

    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    CConfiguration cConf = CConfiguration.create();
    // tests should use the current user for HDFS
    cConf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    cConf.setBoolean(TxConstants.TransactionPruning.PRUNE_ENABLE, false);
    // we want persisting for this test
    cConf.setBoolean(TxConstants.Manager.CFG_DO_PERSIST, true);

    server = TransactionServiceTest.createTxService(zkServer.getConnectionStr(), Networks.getRandomPort(),
                                                    hConf, tmpFolder.newFolder());
    server.startAndWait();

    injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new NonCustomLocationUnitTestModule().getModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new TransactionMetricsModule(),
      new DataFabricModules().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(NamespaceQueryAdmin.class).to(SimpleNamespaceQueryAdmin.class);
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
        }
      },
      Modules.override(new DataSetsModules().getDistributedModules()).with(new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetadataStore.class).to(NoOpMetadataStore.class);
        }
      }),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getNoOpModule());

    zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();

    txStateStorage = injector.getInstance(TransactionStateStorage.class);
    txStateStorage.startAndWait();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    try {
      try {
        server.stopAndWait();
      } finally {
        zkClient.stopAndWait();
        txStateStorage.stopAndWait();
      }
    } finally {
      zkServer.stopAndWait();
      txStateStorage.stopAndWait();
    }
  }

  @Before
  public void resetState() throws Exception {
    TransactionSystemClient txClient = getClient();
    txClient.resetState();
  }

  @Test
  public void testGetSnapshot() throws Exception {
    TransactionSystemClient client = getClient();
    SnapshotCodecProvider codecProvider = new SnapshotCodecProvider(injector.getInstance(Configuration.class));

    Transaction tx1 = client.startShort();
    long currentTime = System.currentTimeMillis();

    TransactionSnapshot snapshot;
    try (InputStream in = client.getSnapshotInputStream()) {
      snapshot = codecProvider.decode(in);
    }
    Assert.assertTrue(snapshot.getTimestamp() >= currentTime);
    Assert.assertTrue(snapshot.getInProgress().containsKey(tx1.getWritePointer()));

    // Ensures that getSnapshot didn't persist a snapshot
    TransactionSnapshot snapshotAfter = getStateStorage().getLatestSnapshot();
    if (snapshotAfter != null) {
      Assert.assertTrue(snapshot.getTimestamp() > snapshotAfter.getTimestamp());
    }
  }

  @Test
  @Override
  public void testNegativeTimeout() throws Exception {
    super.testNegativeTimeout();
  }

  @Test
  @Override
  public void testExcessiveTimeout() throws Exception {
    super.testExcessiveTimeout();
  }
}
