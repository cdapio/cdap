/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.data2.transaction;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.common.service.Services;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.tephra.ChangeId;
import org.apache.tephra.DefaultTransactionExecutor;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionAwares;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TransactionType;
import org.apache.tephra.TxConstants;
import org.apache.tephra.distributed.TransactionService;
import org.apache.tephra.inmemory.InMemoryTransactionService;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.apache.tephra.metrics.TxMetricsCollector;
import org.apache.tephra.persist.HDFSTransactionStateStorage;
import org.apache.tephra.persist.LocalFileTransactionStateStorage;
import org.apache.tephra.persist.TransactionEdit;
import org.apache.tephra.persist.TransactionSnapshot;
import org.apache.tephra.persist.TransactionVisibilityState;
import org.apache.tephra.snapshot.SnapshotCodecProvider;
import org.apache.tephra.visibility.FenceWait;
import org.apache.tephra.visibility.VisibilityFence;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Contract tests verifying Tephra bytecode interactions with Guava APIs (AbstractService,
 * Stopwatch, Files.newOutputStreamSupplier, CountingInputStream, MoreExecutors, Throwables,
 * Objects.toStringHelper) when invoked from CDAP data-fabric.
 */
public class TephraGuavaContractTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testInMemoryAndDistributedTransactionServiceStartStopAndStopFromNew()
      throws Exception {
    Configuration hConf = createBaseTxConf(TEMP_FOLDER.newFolder());

    // 1. Stopping InMemoryTransactionService directly from NEW state
    InMemoryTransactionService unstartedInMem = new InMemoryTransactionService(
        hConf, new InMemoryDiscoveryService(), () -> new TransactionManager(hConf));
    unstartedInMem.stopAsync().awaitTerminated();
    Assert.assertEquals(Service.State.TERMINATED, unstartedInMem.state());

    // 2. Full startAndWait() / stopAndWait() on InMemoryTransactionService
    InMemoryTransactionService runningInMem = new InMemoryTransactionService(
        hConf, new InMemoryDiscoveryService(), () -> new TransactionManager(hConf));
    runningInMem.startAsync().awaitRunning();
    Assert.assertEquals(Service.State.RUNNING, runningInMem.state());
    runningInMem.stopAsync().awaitTerminated();
    Assert.assertEquals(Service.State.TERMINATED, runningInMem.state());

    // 3. Distributed TransactionService: stop from NEW and full leader election start/stop
    InMemoryZKServer zkServer = InMemoryZKServer.builder().setDataDir(TEMP_FOLDER.newFolder()).build();
    zkServer.startAsync().awaitRunning();
    try {
      ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      zkClient.startAsync().awaitRunning();
      try {
        TransactionService unstartedDist = new TransactionService(
            hConf, zkClient, new InMemoryDiscoveryService(), () -> new TransactionManager(hConf));
        unstartedDist.stopAsync().awaitTerminated();
        Assert.assertEquals(Service.State.TERMINATED, unstartedDist.state());

        TransactionService runningDist = new TransactionService(
            hConf, zkClient, new InMemoryDiscoveryService(), () -> new TransactionManager(hConf));
        runningDist.startAsync().awaitRunning();
        Assert.assertEquals(Service.State.RUNNING, runningDist.state());
        // Wait briefly for leader election callback to start underlying TransactionManager
        long deadline = System.currentTimeMillis() + 5000L;
        while ((runningDist.getTransactionManager() == null
            || runningDist.getTransactionManager().state() != Service.State.RUNNING)
            && System.currentTimeMillis() < deadline) {
          TimeUnit.MILLISECONDS.sleep(20);
        }
        Assert.assertNotNull(runningDist.getTransactionManager());
        Assert.assertEquals(Service.State.RUNNING, runningDist.getTransactionManager().state());
        runningDist.stopAsync().awaitTerminated();
        Assert.assertEquals(Service.State.TERMINATED, runningDist.state());
      } finally {
        zkClient.stopAsync().awaitTerminated();
      }
    } finally {
      zkServer.stopAsync().awaitTerminated();
    }
  }

  @Test
  public void testTransactionManagerWithLocalFileStorageAndSnapshotEditToString() throws Exception {
    File localSnapshotDir = TEMP_FOLDER.newFolder();
    Configuration hConf = createBaseTxConf(localSnapshotDir);
    SnapshotCodecProvider codecProvider = new SnapshotCodecProvider(hConf);
    LocalFileTransactionStateStorage storage =
        new LocalFileTransactionStateStorage(hConf, codecProvider, new TxMetricsCollector());

    TransactionManager txManager =
        new TransactionManager(hConf, storage, new TxMetricsCollector());
    txManager.startAsync().awaitRunning();
    Assert.assertEquals(Service.State.RUNNING, txManager.state());
    try {
      Transaction tx1 = txManager.startShort("clientA");
      Transaction checkpointed = txManager.checkpoint(tx1);
      byte[] changeKey = "row-1".getBytes(StandardCharsets.UTF_8);
      txManager.canCommit(checkpointed.getTransactionId(), Collections.singleton(changeKey));
      txManager.commit(checkpointed.getTransactionId(), checkpointed.getWritePointer());

      Transaction tx2 = txManager.startShort("clientB");
      txManager.invalidate(tx2.getTransactionId());

      TransactionSnapshot current = txManager.getCurrentState();
      Assert.assertNotNull(current);
      String snapshotStr = current.toString();
      Assert.assertTrue(snapshotStr.contains("timestamp="));
      Assert.assertTrue(snapshotStr.contains("readPointer="));

      storage.writeSnapshot(current);
      List<String> snapshots = storage.listSnapshots();
      Assert.assertFalse(snapshots.isEmpty());

      TransactionSnapshot reloaded = storage.getLatestSnapshot();
      Assert.assertNotNull(reloaded);
      Assert.assertEquals(current.getWritePointer(), reloaded.getWritePointer());
      Assert.assertEquals(current.getInvalid(), reloaded.getInvalid());

      // Exercise both org.apache.tephra.persist.TransactionEdit and co.cask.tephra.persist.TransactionEdit toString()
      TransactionEdit apacheEdit = TransactionEdit.createCommitted(
          checkpointed.getTransactionId(),
          Collections.singleton(new ChangeId(changeKey)),
          checkpointed.getWritePointer(),
          true);
      Assert.assertTrue(apacheEdit.toString().contains("COMMITTED"));

      co.cask.tephra.persist.TransactionEdit caskEdit =
          co.cask.tephra.persist.TransactionEdit.createStarted(
              100L, 90L, System.currentTimeMillis() + 10000L, TransactionType.SHORT);
      Assert.assertTrue(caskEdit.toString().contains("INPROGRESS"));
    } finally {
      txManager.stopAsync().awaitTerminated();
      Assert.assertEquals(Service.State.TERMINATED, txManager.state());
    }
  }

  @Test
  public void testHdfsTransactionStateStorageLifecycleAndCountingInputStream() throws Exception {
    File hdfsSimDir = TEMP_FOLDER.newFolder();
    Configuration hConf = createBaseTxConf(hdfsSimDir);
    hConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "file:///");
    hConf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_DIR, hdfsSimDir.getAbsolutePath());

    SnapshotCodecProvider codecProvider = new SnapshotCodecProvider(hConf);
    HDFSTransactionStateStorage unstartedHdfsStorage =
        new HDFSTransactionStateStorage(hConf, codecProvider, new TxMetricsCollector());
    unstartedHdfsStorage.stopAsync().awaitTerminated();
    Assert.assertEquals(Service.State.TERMINATED, unstartedHdfsStorage.state());

    HDFSTransactionStateStorage hdfsStorage =
        new HDFSTransactionStateStorage(hConf, codecProvider, new TxMetricsCollector());
    hdfsStorage.startAsync().awaitRunning();
    Assert.assertEquals(Service.State.RUNNING, hdfsStorage.state());
    try {
      hdfsStorage.setupStorage();
      TransactionSnapshot snapshot = new TransactionSnapshot(
          System.currentTimeMillis(), 10L, 20L,
          Collections.singletonList(15L), Collections.emptyNavigableMap());
      hdfsStorage.writeSnapshot(snapshot);

      List<String> snapshots = hdfsStorage.listSnapshots();
      Assert.assertEquals(1, snapshots.size());

      // Exercises CountingInputStream inside HDFSTransactionStateStorage
      TransactionSnapshot latestSnapshot = hdfsStorage.getLatestSnapshot();
      Assert.assertNotNull(latestSnapshot);
      Assert.assertEquals(20L, latestSnapshot.getWritePointer());

      TransactionVisibilityState visState = hdfsStorage.getLatestTransactionVisibilityState();
      Assert.assertNotNull(visState);
      Assert.assertEquals(10L, visState.getReadPointer());
    } finally {
      hdfsStorage.stopAsync().awaitTerminated();
      Assert.assertEquals(Service.State.TERMINATED, hdfsStorage.state());
    }
  }

  @Test
  public void testDefaultTransactionExecutorAndDefaultFenceWait() throws Exception {
    Configuration hConf = createBaseTxConf(TEMP_FOLDER.newFolder());
    TransactionManager txManager = new TransactionManager(hConf);
    txManager.startAsync().awaitRunning();
    Assert.assertEquals(Service.State.RUNNING, txManager.state());
    try {
      TransactionSystemClient txClient = new InMemoryTxSystemClient(txManager);
      TransactionAware emptyAware = TransactionAwares.of(Collections.emptyList());

      DefaultTransactionExecutor executor = new DefaultTransactionExecutor(
          txClient, Collections.singletonList(emptyAware), RetryStrategies.noRetries());

      // Happy path execute, executeUnchecked, and submit
      Assert.assertEquals("done", executor.execute(() -> "done"));
      Assert.assertEquals(Integer.valueOf(9),
          executor.executeUnchecked(x -> x + 4, 5));
      Assert.assertEquals("asyncDone", executor.submit(() -> "asyncDone").get());

      // Unchecked exception propagation in execute and executeUnchecked
      final IllegalArgumentException expectedUnchecked =
          new IllegalArgumentException("bad tx input");
      try {
        executor.execute((TransactionExecutor.Subroutine) () -> {
          throw expectedUnchecked;
        });
        Assert.fail("Expected TransactionFailureException");
      } catch (TransactionFailureException actual) {
        Assert.assertSame(expectedUnchecked, actual.getCause());
      }

      try {
        executor.executeUnchecked((TransactionExecutor.Subroutine) () -> {
          throw expectedUnchecked;
        });
        Assert.fail("Expected RuntimeException from executeUnchecked");
      } catch (RuntimeException actual) {
        Assert.assertTrue(actual.getCause() instanceof TransactionFailureException);
        Assert.assertSame(expectedUnchecked, actual.getCause().getCause());
      }

      // DefaultFenceWait + ReadFence + WriteFence: exercises Stopwatch.elapsedTime(TimeUnit),
      // UnsignedBytes.lexicographicalComparator(), Bytes.concat, and Longs.toByteArray
      byte[] fenceId = "table-fence-1".getBytes(StandardCharsets.UTF_8);
      TransactionContext preFenceReaderCtx =
          new TransactionContext(txClient, VisibilityFence.create(fenceId));
      preFenceReaderCtx.start();
      FenceWait cleanWait = VisibilityFence.prepareWait(fenceId, txClient);
      cleanWait.await(1, TimeUnit.SECONDS);
      try {
        preFenceReaderCtx.finish();
        Assert.fail("Expected pre-fence reader to conflict after WriteFence committed");
      } catch (org.apache.tephra.TransactionConflictException expected) {
        preFenceReaderCtx.abort();
      }

      // Reader started after fence completes commits cleanly
      TransactionContext postFenceReaderCtx =
          new TransactionContext(txClient, VisibilityFence.create(fenceId));
      postFenceReaderCtx.start();
      postFenceReaderCtx.finish();

      // Exercise DefaultFenceWait conflict retry + TimeoutException when fence commit keeps conflicting
      TransactionSystemClient alwaysConflictingClient = org.mockito.Mockito.spy(txClient);
      org.mockito.Mockito.doThrow(
          new org.apache.tephra.TransactionConflictException(1L, "fenceConflict", "writer"))
          .when(alwaysConflictingClient)
          .canCommitOrThrow(org.mockito.Mockito.any(), org.mockito.Mockito.anyCollection());

      FenceWait timeoutWait = VisibilityFence.prepareWait(fenceId, alwaysConflictingClient);
      try {
        timeoutWait.await(30, TimeUnit.MILLISECONDS);
        Assert.fail("Expected TimeoutException when fence commit repeatedly conflicts");
      } catch (TimeoutException expected) {
        // expected
      }
    } finally {
      txManager.stopAsync().awaitTerminated();
      Assert.assertEquals(Service.State.TERMINATED, txManager.state());
    }

    // Verify startAndWait / stopAndWait state transitions on NEW and RUNNING instances (F11)
    TransactionManager unstartedManager = new TransactionManager(createBaseTxConf(TEMP_FOLDER.newFolder()));
    Assert.assertEquals(Service.State.TERMINATED, Services.stopAndWait(unstartedManager));

    TransactionManager waitManager = new TransactionManager(createBaseTxConf(TEMP_FOLDER.newFolder()));
    Assert.assertEquals(Service.State.RUNNING, Services.startAndWait(waitManager));
    Assert.assertEquals(Service.State.RUNNING, Services.startAndWait(waitManager));
    Assert.assertEquals(Service.State.TERMINATED, Services.stopAndWait(waitManager));

    // Verify shaded TephraZKClientService nested parent creation callbacks (F2)
    org.apache.twill.internal.zookeeper.InMemoryZKServer zkServer =
        org.apache.twill.internal.zookeeper.InMemoryZKServer.builder()
            .setDataDir(TEMP_FOLDER.newFolder())
            .build();
    zkServer.startAsync().awaitRunning();
    try {
      org.apache.tephra.zookeeper.TephraZKClientService tephraZk =
          new org.apache.tephra.zookeeper.TephraZKClientService(
              zkServer.getConnectionStr(), 10000, null, com.google.common.collect.ArrayListMultimap.create());
      tephraZk.startAsync().awaitRunning();
      try {
        String createdPath = tephraZk.create(
            "/tephra/nested/parent/child", "payload".getBytes(StandardCharsets.UTF_8),
            org.apache.zookeeper.CreateMode.PERSISTENT, true).get(5, TimeUnit.SECONDS);
        Assert.assertEquals("/tephra/nested/parent/child", createdPath);
      } finally {
        tephraZk.stopAsync().awaitTerminated();
      }
    } finally {
      zkServer.stopAsync().awaitTerminated();
    }
  }

  private static Configuration createBaseTxConf(File snapshotDir) {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TxConstants.Manager.CFG_DO_PERSIST, true);
    conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR, snapshotDir.getAbsolutePath());
    conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_DIR, snapshotDir.getAbsolutePath());
    conf.set(TxConstants.Service.CFG_DATA_TX_BIND_ADDRESS, "127.0.0.1");
    conf.setInt(TxConstants.Service.CFG_DATA_TX_BIND_PORT, 0);
    return conf;
  }
}
