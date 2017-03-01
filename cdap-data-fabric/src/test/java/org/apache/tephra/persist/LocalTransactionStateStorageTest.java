/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.persist;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.ChangeId;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionType;
import org.apache.tephra.TxConstants;
import org.apache.tephra.metrics.TxMetricsCollector;
import org.apache.tephra.snapshot.DefaultSnapshotCodec;
import org.apache.tephra.snapshot.SnapshotCodecProvider;
import org.apache.tephra.snapshot.SnapshotCodecV4;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Runs transaction persistence tests against the {@link LocalFileTransactionStateStorage} and
 * {@link LocalFileTransactionLog} implementations.
 */
public class LocalTransactionStateStorageTest extends AbstractTransactionStateStorageTest {
  @ClassRule
  public static TemporaryFolder tmpDir = new TemporaryFolder();

  @Override
  protected Configuration getConfiguration(String testName) throws IOException {
    File testDir = tmpDir.newFolder(testName);
    Configuration conf = new Configuration();
    conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR, testDir.getAbsolutePath());
    conf.set(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES, SnapshotCodecV4.class.getName());
    return conf;
  }

  @Override
  protected AbstractTransactionStateStorage getStorage(Configuration conf) {
    return new LocalFileTransactionStateStorage(conf, new SnapshotCodecProvider(conf), new TxMetricsCollector());
  }

  // v2 TransactionEdit
  @SuppressWarnings("deprecation")
  private class TransactionEditV2 extends TransactionEdit {
    public TransactionEditV2(long writePointer, long visibilityUpperBound, State state, long expirationDate,
                             Set<ChangeId> changes, long commitPointer, boolean canCommit, TransactionType type) {
      super(writePointer, visibilityUpperBound, state, expirationDate, changes, commitPointer, canCommit, type, 
            null, 0L, 0L, null);
    }
    @Override
    public void write(DataOutput out) throws IOException {
      TransactionEditCodecs.encode(this, out, new TransactionEditCodecs.TransactionEditCodecV2());
    }
  }

  // Note: this test cannot run in AbstractTransactionStateStorageTest, since SequenceFile throws exception saying
  // TransactionEditV2 is not TransactionEdit. Since the code path this test is verifying is the same path between
  // HDFS and Local Storage, having this only over here is fine.
  @SuppressWarnings("deprecation")
  @Test
  public void testLongTxnBackwardsCompatibility() throws Exception {
    Configuration conf = getConfiguration("testLongTxnBackwardsCompatibility");

    // Use SnapshotCodec version 1
    String latestSnapshotCodec = conf.get(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES);
    conf.set(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES, DefaultSnapshotCodec.class.getName());

    TransactionStateStorage storage = null;
    try {
      storage = getStorage(conf);
      storage.startAndWait();

      // Create transaction snapshot and transaction edits with version when long running txns had -1 expiration.
      Collection<Long> invalid = Lists.newArrayList();
      NavigableMap<Long, TransactionManager.InProgressTx> inProgress = Maps.newTreeMap();
      long time1 = System.currentTimeMillis();
      long wp1 = time1 * TxConstants.MAX_TX_PER_MS;
      inProgress.put(wp1, new TransactionManager.InProgressTx(wp1 - 5, -1L));
      long time2 = time1 + 100;
      long wp2 = time2 * TxConstants.MAX_TX_PER_MS;
      inProgress.put(wp2, new TransactionManager.InProgressTx(wp2 - 50, time2 + 1000));
      Map<Long, Set<ChangeId>> committing = Maps.newHashMap();
      Map<Long, Set<ChangeId>> committed = Maps.newHashMap();
      TransactionSnapshot snapshot = new TransactionSnapshot(time2, 0, wp2, invalid,
                                                             inProgress, committing, committed);
      long time3 = time1 + 200;
      long wp3 = time3 * TxConstants.MAX_TX_PER_MS;
      TransactionEdit edit1 = new TransactionEditV2(wp3, wp3 - 10, TransactionEdit.State.INPROGRESS, -1L,
                                                    null, 0L, false, null);
      long time4 = time1 + 300;
      long wp4 = time4  * TxConstants.MAX_TX_PER_MS;
      TransactionEdit edit2 = new TransactionEditV2(wp4, wp4 - 10, TransactionEdit.State.INPROGRESS, time4 + 1000,
                                                    null, 0L, false, null);

      // write snapshot and transaction edit
      storage.writeSnapshot(snapshot);
      TransactionLog log = storage.createLog(time2);
      log.append(edit1);
      log.append(edit2);
      log.close();

      // Start transaction manager
      conf.set(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES, latestSnapshotCodec);
      long longTimeout = TimeUnit.SECONDS.toMillis(conf.getLong(TxConstants.Manager.CFG_TX_LONG_TIMEOUT,
                                                                TxConstants.Manager.DEFAULT_TX_LONG_TIMEOUT));
      TransactionManager txm = new TransactionManager(conf, storage, new TxMetricsCollector());
      txm.startAndWait();
      try {
        // Verify that the txns in old format were read correctly.
        // There should be four in-progress transactions, and no invalid transactions
        TransactionSnapshot snapshot1 = txm.getCurrentState();
        Assert.assertEquals(ImmutableSortedSet.of(wp1, wp2, wp3, wp4), snapshot1.getInProgress().keySet());
        verifyInProgress(snapshot1.getInProgress().get(wp1), TransactionType.LONG, time1 + longTimeout);
        verifyInProgress(snapshot1.getInProgress().get(wp2), TransactionType.SHORT, time2 + 1000);
        verifyInProgress(snapshot1.getInProgress().get(wp3), TransactionType.LONG, time3 + longTimeout);
        verifyInProgress(snapshot1.getInProgress().get(wp4), TransactionType.SHORT, time4 + 1000);
        Assert.assertEquals(0, snapshot1.getInvalid().size());
      } finally {
        txm.stopAndWait();
      }
    } finally {
      if (storage != null) {
        storage.stopAndWait();
      }
    }
  }

  // Note: this test cannot run in AbstractTransactionStateStorageTest, since SequenceFile throws exception saying
  // TransactionEditV2 is not TransactionEdit. Since the code path this test is verifying is the same path between
  // HDFS and Local Storage, having this only over here is fine.
  @SuppressWarnings("deprecation")
  @Test
  public void testAbortEditBackwardsCompatibility() throws Exception {
    Configuration conf = getConfiguration("testAbortEditBackwardsCompatibility");

    TransactionStateStorage storage = null;
    try {
      storage = getStorage(conf);
      storage.startAndWait();

      // Create edits for transaction type addition to abort
      long time1 = System.currentTimeMillis();
      long wp1 = time1 * TxConstants.MAX_TX_PER_MS;
      TransactionEdit edit1 = new TransactionEditV2(wp1, wp1 - 10, TransactionEdit.State.INPROGRESS, -1L,
                                                    null, 0L, false, null);
      TransactionEdit edit2 = new TransactionEditV2(wp1, 0L, TransactionEdit.State.ABORTED, 0L,
                                                    null, 0L, false, null);

      long time2 = time1 + 400;
      long wp2 = time2 * TxConstants.MAX_TX_PER_MS;
      TransactionEdit edit3 = new TransactionEditV2(wp2, wp2 - 10, TransactionEdit.State.INPROGRESS, time2 + 10000,
                                                    null, 0L, false, null);
      TransactionEdit edit4 = new TransactionEditV2(wp2, 0L, TransactionEdit.State.INVALID, 0L, null, 0L, false, null);
      // Simulate case where we cannot determine txn state during abort
      TransactionEdit edit5 = new TransactionEditV2(wp2, 0L, TransactionEdit.State.ABORTED, 0L, null, 0L, false, null);

      // write snapshot and transaction edit
      TransactionLog log = storage.createLog(time1);
      log.append(edit1);
      log.append(edit2);
      log.append(edit3);
      log.append(edit4);
      log.append(edit5);
      log.close();

      // Start transaction manager
      TransactionManager txm = new TransactionManager(conf, storage, new TxMetricsCollector());
      txm.startAndWait();
      try {
        // Verify that the txns in old format were read correctly.
        // Both transactions should be in invalid state
        TransactionSnapshot snapshot1 = txm.getCurrentState();
        Assert.assertEquals(ImmutableList.of(wp1, wp2), snapshot1.getInvalid());
        Assert.assertEquals(0, snapshot1.getInProgress().size());
        Assert.assertEquals(0, snapshot1.getCommittedChangeSets().size());
        Assert.assertEquals(0, snapshot1.getCommittingChangeSets().size());
      } finally {
        txm.stopAndWait();
      }
    } finally {
      if (storage != null) {
        storage.stopAndWait();
      }
    }
  }

  private void verifyInProgress(TransactionManager.InProgressTx inProgressTx, TransactionType type,
                                long expiration) throws Exception {
    Assert.assertEquals(type, inProgressTx.getType());
    Assert.assertTrue(inProgressTx.getExpiration() == expiration);
  }
}
