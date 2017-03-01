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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.ChangeId;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionType;
import org.apache.tephra.TxConstants;
import org.apache.tephra.metrics.TxMetricsCollector;
import org.apache.tephra.util.TransactionEditUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Commons tests to run against the {@link TransactionStateStorage} implementations.
 */
public abstract class AbstractTransactionStateStorageTest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractTransactionStateStorageTest.class);
  private static Random random = new Random();

  protected abstract Configuration getConfiguration(String testName) throws IOException;

  protected abstract AbstractTransactionStateStorage getStorage(Configuration conf);

  @Test
  public void testSnapshotPersistence() throws Exception {
    Configuration conf = getConfiguration("testSnapshotPersistence");

    TransactionSnapshot snapshot = createRandomSnapshot();
    TransactionStateStorage storage = getStorage(conf);
    try {
      storage.startAndWait();
      storage.writeSnapshot(snapshot);

      TransactionSnapshot readSnapshot = storage.getLatestSnapshot();
      assertNotNull(readSnapshot);
      assertEquals(snapshot, readSnapshot);
    } finally {
      storage.stopAndWait();
    }
  }

  @Test
  public void testLogWriteAndRead() throws Exception {
    Configuration conf = getConfiguration("testLogWriteAndRead");

    // create some random entries
    List<TransactionEdit> edits = TransactionEditUtil.createRandomEdits(100);
    TransactionStateStorage storage = getStorage(conf);
    try {
      long now = System.currentTimeMillis();
      storage.startAndWait();
      TransactionLog log = storage.createLog(now);
      for (TransactionEdit edit : edits) {
        log.append(edit);
      }
      log.close();

      Collection<TransactionLog> logsToRead = storage.getLogsSince(now);
      // should only be our one log
      assertNotNull(logsToRead);
      assertEquals(1, logsToRead.size());
      TransactionLogReader logReader = logsToRead.iterator().next().getReader();
      assertNotNull(logReader);

      List<TransactionEdit> readEdits = Lists.newArrayListWithExpectedSize(edits.size());
      TransactionEdit nextEdit;
      while ((nextEdit = logReader.next()) != null) {
        readEdits.add(nextEdit);
      }
      logReader.close();
      assertEquals(edits.size(), readEdits.size());
      for (int i = 0; i < edits.size(); i++) {
        LOG.info("Checking edit " + i);
        assertEquals(edits.get(i), readEdits.get(i));
      }
    } finally {
      storage.stopAndWait();
    }
  }

  @Test
  public void testTransactionManagerPersistence() throws Exception {
    Configuration conf = getConfiguration("testTransactionManagerPersistence");
    conf.setInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, 0); // no cleanup thread
    // start snapshot thread, but with long enough interval so we only get snapshots on shutdown
    conf.setInt(TxConstants.Manager.CFG_TX_SNAPSHOT_INTERVAL, 600);

    TransactionStateStorage storage = null;
    TransactionStateStorage storage2 = null;
    TransactionStateStorage storage3 = null;
    try {
      storage = getStorage(conf);
      TransactionManager txManager = new TransactionManager
        (conf, storage, new TxMetricsCollector());
      txManager.startAndWait();

      // TODO: replace with new persistence tests
      final byte[] a = { 'a' };
      final byte[] b = { 'b' };
      // start a tx1, add a change A and commit
      Transaction tx1 = txManager.startShort();
      Assert.assertTrue(txManager.canCommit(tx1, Collections.singleton(a)));
      Assert.assertTrue(txManager.commit(tx1));
      // start a tx2 and add a change B
      Transaction tx2 = txManager.startShort();
      Assert.assertTrue(txManager.canCommit(tx2, Collections.singleton(b)));
      // start a tx3
      Transaction tx3 = txManager.startShort();
      // restart
      txManager.stopAndWait();
      TransactionSnapshot origState = txManager.getCurrentState();
      LOG.info("Orig state: " + origState);

      Thread.sleep(100);
      // starts a new tx manager
      storage2 = getStorage(conf);
      txManager = new TransactionManager(conf, storage2, new TxMetricsCollector());
      txManager.startAndWait();

      // check that the reloaded state matches the old
      TransactionSnapshot newState = txManager.getCurrentState();
      LOG.info("New state: " + newState);
      assertEquals(origState, newState);

      // commit tx2
      Assert.assertTrue(txManager.commit(tx2));
      // start another transaction, must be greater than tx3
      Transaction tx4 = txManager.startShort();
      Assert.assertTrue(tx4.getTransactionId() > tx3.getTransactionId());
      // tx1 must be visble from tx2, but tx3 and tx4 must not
      Assert.assertTrue(tx2.isVisible(tx1.getTransactionId()));
      Assert.assertFalse(tx2.isVisible(tx3.getTransactionId()));
      Assert.assertFalse(tx2.isVisible(tx4.getTransactionId()));
      // add same change for tx3
      Assert.assertFalse(txManager.canCommit(tx3, Collections.singleton(b)));
      // check visibility with new xaction
      Transaction tx5 = txManager.startShort();
      Assert.assertTrue(tx5.isVisible(tx1.getTransactionId()));
      Assert.assertTrue(tx5.isVisible(tx2.getTransactionId()));
      Assert.assertFalse(tx5.isVisible(tx3.getTransactionId()));
      Assert.assertFalse(tx5.isVisible(tx4.getTransactionId()));
      // can commit tx3?
      txManager.abort(tx3);
      txManager.abort(tx4);
      txManager.abort(tx5);
      // start new tx and verify its exclude list is empty
      Transaction tx6 = txManager.startShort();
      Assert.assertFalse(tx6.hasExcludes());
      txManager.abort(tx6);

      // now start 5 x claim size transactions
      Transaction tx = txManager.startShort();
      for (int i = 1; i < 50; i++) {
        tx = txManager.startShort();
      }
      origState = txManager.getCurrentState();

      Thread.sleep(100);
      // simulate crash by starting a new tx manager without a stopAndWait
      storage3 = getStorage(conf);
      txManager = new TransactionManager(conf, storage3, new TxMetricsCollector());
      txManager.startAndWait();

      // verify state again matches (this time should include WAL replay)
      newState = txManager.getCurrentState();
      assertEquals(origState, newState);

      // get a new transaction and verify it is greater
      Transaction txAfter = txManager.startShort();
      Assert.assertTrue(txAfter.getTransactionId() > tx.getTransactionId());
    } finally {
      if (storage != null) {
        storage.stopAndWait();
      }
      if (storage2 != null) {
        storage2.stopAndWait();
      }
      if (storage3 != null) {
        storage3.stopAndWait();
      }
    }
  }

  /**
   * Tests whether the committed set is advanced properly on WAL replay.
   */
  @Test
  public void testCommittedSetClearing() throws Exception {
    Configuration conf = getConfiguration("testCommittedSetClearing");
    conf.setInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, 0); // no cleanup thread
    conf.setInt(TxConstants.Manager.CFG_TX_SNAPSHOT_INTERVAL, 0); // no periodic snapshots

    TransactionStateStorage storage1 = null;
    TransactionStateStorage storage2 = null;
    try {
      storage1 = getStorage(conf);
      TransactionManager txManager = new TransactionManager
        (conf, storage1, new TxMetricsCollector());
      txManager.startAndWait();

      // TODO: replace with new persistence tests
      final byte[] a = { 'a' };
      final byte[] b = { 'b' };
      // start a tx1, add a change A and commit
      Transaction tx1 = txManager.startShort();
      Assert.assertTrue(txManager.canCommit(tx1, Collections.singleton(a)));
      Assert.assertTrue(txManager.commit(tx1));
      // start a tx2 and add a change B
      Transaction tx2 = txManager.startShort();
      Assert.assertTrue(txManager.canCommit(tx2, Collections.singleton(b)));
      // start a tx3
      Transaction tx3 = txManager.startShort();
      TransactionSnapshot origState = txManager.getCurrentState();
      LOG.info("Orig state: " + origState);

      // simulate a failure by starting a new tx manager without stopping first
      storage2 = getStorage(conf);
      txManager = new TransactionManager(conf, storage2, new TxMetricsCollector());
      txManager.startAndWait();

      // check that the reloaded state matches the old
      TransactionSnapshot newState = txManager.getCurrentState();
      LOG.info("New state: " + newState);
      assertEquals(origState, newState);

    } finally {
      if (storage1 != null) {
        storage1.stopAndWait();
      }
      if (storage2 != null) {
        storage2.stopAndWait();
      }
    }
  }

  /**
   * Tests removal of old snapshots and old transaction logs.
   */
  @Test
  public void testOldFileRemoval() throws Exception {
    Configuration conf = getConfiguration("testOldFileRemoval");
    TransactionStateStorage storage = null;
    try {
      storage = getStorage(conf);
      storage.startAndWait();
      long now = System.currentTimeMillis();
      long writePointer = 1;
      Collection<Long> invalid = Lists.newArrayList();
      NavigableMap<Long, TransactionManager.InProgressTx> inprogress = Maps.newTreeMap();
      Map<Long, Set<ChangeId>> committing = Maps.newHashMap();
      Map<Long, Set<ChangeId>> committed = Maps.newHashMap();
      TransactionSnapshot snapshot = new TransactionSnapshot(now, 0, writePointer++, invalid,
                                                             inprogress, committing, committed);
      TransactionEdit dummyEdit = TransactionEdit.createStarted(1, 0, Long.MAX_VALUE, TransactionType.SHORT);

      // write snapshot 1
      storage.writeSnapshot(snapshot);
      TransactionLog log = storage.createLog(now);
      log.append(dummyEdit);
      log.close();

      snapshot = new TransactionSnapshot(now + 1, 0, writePointer++, invalid, inprogress, committing, committed);
      // write snapshot 2
      storage.writeSnapshot(snapshot);
      log = storage.createLog(now + 1);
      log.append(dummyEdit);
      log.close();

      snapshot = new TransactionSnapshot(now + 2, 0, writePointer++, invalid, inprogress, committing, committed);
      // write snapshot 3
      storage.writeSnapshot(snapshot);
      log = storage.createLog(now + 2);
      log.append(dummyEdit);
      log.close();

      snapshot = new TransactionSnapshot(now + 3, 0, writePointer++, invalid, inprogress, committing, committed);
      // write snapshot 4
      storage.writeSnapshot(snapshot);
      log = storage.createLog(now + 3);
      log.append(dummyEdit);
      log.close();

      snapshot = new TransactionSnapshot(now + 4, 0, writePointer++, invalid, inprogress, committing, committed);
      // write snapshot 5
      storage.writeSnapshot(snapshot);
      log = storage.createLog(now + 4);
      log.append(dummyEdit);
      log.close();

      snapshot = new TransactionSnapshot(now + 5, 0, writePointer++, invalid, inprogress, committing, committed);
      // write snapshot 6
      storage.writeSnapshot(snapshot);
      log = storage.createLog(now + 5);
      log.append(dummyEdit);
      log.close();

      List<String> allSnapshots = storage.listSnapshots();
      LOG.info("All snapshots: " + allSnapshots);
      assertEquals(6, allSnapshots.size());
      List<String> allLogs = storage.listLogs();
      LOG.info("All logs: " + allLogs);
      assertEquals(6, allLogs.size());

      long oldestKept = storage.deleteOldSnapshots(3);
      assertEquals(now + 3, oldestKept);
      allSnapshots = storage.listSnapshots();
      LOG.info("All snapshots: " + allSnapshots);
      assertEquals(3, allSnapshots.size());

      storage.deleteLogsOlderThan(oldestKept);
      allLogs = storage.listLogs();
      LOG.info("All logs: " + allLogs);
      assertEquals(3, allLogs.size());
    } finally {
      if (storage != null) {
        storage.stopAndWait();
      }
    }
  }
  
  @Test
  public void testLongTxnEditReplay() throws Exception {
    Configuration conf = getConfiguration("testLongTxnEditReplay");
    TransactionStateStorage storage = null;
    try {
      storage = getStorage(conf);
      storage.startAndWait();

      // Create long running txns. Abort one of them, invalidate another, invalidate and abort the last.
      long time1 = System.currentTimeMillis();
      long wp1 = time1 * TxConstants.MAX_TX_PER_MS;
      TransactionEdit edit1 = TransactionEdit.createStarted(wp1, wp1 - 10, time1 + 100000, TransactionType.LONG);
      TransactionEdit edit2 = TransactionEdit.createAborted(wp1, TransactionType.LONG, null);

      long time2 = time1 + 100;
      long wp2 = time2 * TxConstants.MAX_TX_PER_MS;
      TransactionEdit edit3 = TransactionEdit.createStarted(wp2, wp2 - 10, time2 + 100000, TransactionType.LONG);
      TransactionEdit edit4 = TransactionEdit.createInvalid(wp2);

      long time3 = time1 + 200;
      long wp3 = time3 * TxConstants.MAX_TX_PER_MS;
      TransactionEdit edit5 = TransactionEdit.createStarted(wp3, wp3 - 10, time3 + 100000, TransactionType.LONG);
      TransactionEdit edit6 = TransactionEdit.createInvalid(wp3);
      TransactionEdit edit7 = TransactionEdit.createAborted(wp3, TransactionType.LONG, null);
      
      // write transaction edits
      TransactionLog log = storage.createLog(time1);
      log.append(edit1);
      log.append(edit2);
      log.append(edit3);
      log.append(edit4);
      log.append(edit5);
      log.append(edit6);
      log.append(edit7);
      log.close();

      // Start transaction manager
      TransactionManager txm = new TransactionManager(conf, storage, new TxMetricsCollector());
      txm.startAndWait();
      try {
        // Verify that all txns are in invalid list.
        TransactionSnapshot snapshot1 = txm.getCurrentState();
        assertEquals(ImmutableList.of(wp1, wp2, wp3), snapshot1.getInvalid());
        assertEquals(0, snapshot1.getInProgress().size());
        assertEquals(0, snapshot1.getCommittedChangeSets().size());
        assertEquals(0, snapshot1.getCommittedChangeSets().size());
      } finally {
        txm.stopAndWait();
      }
    } finally {
      if (storage != null) {
        storage.stopAndWait();
      }
    }
  }

  @Test
  public void testTruncateInvalidTxEditReplay() throws Exception {
    Configuration conf = getConfiguration("testTruncateInvalidTxEditReplay");
    TransactionStateStorage storage = null;
    try {
      storage = getStorage(conf);
      storage.startAndWait();

      // Create some txns, and invalidate all of them.
      long time1 = System.currentTimeMillis();
      long wp1 = time1 * TxConstants.MAX_TX_PER_MS;
      TransactionEdit edit1 = TransactionEdit.createStarted(wp1, wp1 - 10, time1 + 100000, TransactionType.LONG);
      TransactionEdit edit2 = TransactionEdit.createInvalid(wp1);

      long time2 = time1 + 100;
      long wp2 = time2 * TxConstants.MAX_TX_PER_MS;
      TransactionEdit edit3 = TransactionEdit.createStarted(wp2, wp2 - 10, time2 + 10000, TransactionType.SHORT);
      TransactionEdit edit4 = TransactionEdit.createInvalid(wp2);

      long time3 = time1 + 2000;
      long wp3 = time3 * TxConstants.MAX_TX_PER_MS;
      TransactionEdit edit5 = TransactionEdit.createStarted(wp3, wp3 - 10, time3 + 100000, TransactionType.LONG);
      TransactionEdit edit6 = TransactionEdit.createInvalid(wp3);

      long time4 = time1 + 2100;
      long wp4 = time4 * TxConstants.MAX_TX_PER_MS;
      TransactionEdit edit7 = TransactionEdit.createStarted(wp4, wp4 - 10, time4 + 10000, TransactionType.SHORT);
      TransactionEdit edit8 = TransactionEdit.createInvalid(wp4);

      // remove wp1 and wp3 from invalid list
      TransactionEdit edit9 = TransactionEdit.createTruncateInvalidTx(ImmutableSet.of(wp1, wp3));
      // truncate invalid transactions before time3
      TransactionEdit edit10 = TransactionEdit.createTruncateInvalidTxBefore(time3);

      // write transaction edits
      TransactionLog log = storage.createLog(time1);
      log.append(edit1);
      log.append(edit2);
      log.append(edit3);
      log.append(edit4);
      log.append(edit5);
      log.append(edit6);
      log.append(edit7);
      log.append(edit8);
      log.append(edit9);
      log.append(edit10);
      log.close();

      // Start transaction manager
      TransactionManager txm = new TransactionManager(conf, storage, new TxMetricsCollector());
      txm.startAndWait();
      try {
        // Only wp4 should be in invalid list.
        TransactionSnapshot snapshot = txm.getCurrentState();
        assertEquals(ImmutableList.of(wp4), snapshot.getInvalid());
        assertEquals(0, snapshot.getInProgress().size());
        assertEquals(0, snapshot.getCommittedChangeSets().size());
        assertEquals(0, snapshot.getCommittedChangeSets().size());
      } finally {
        txm.stopAndWait();
      }
    } finally {
      if (storage != null) {
        storage.stopAndWait();
      }
    }
  }

  /**
   * Generates a new snapshot object with semi-randomly populated values.  This does not necessarily accurately
   * represent a typical snapshot's distribution of values, as we only set an upper bound on pointer values.
   *
   * We generate a new snapshot with the contents:
   * <ul>
   *   <li>readPointer = 1M + (random % 1M)</li>
   *   <li>writePointer = readPointer + 1000</li>
   *   <li>waterMark = writePointer + 1000</li>
   *   <li>inProgress = one each for (writePointer - 500)..writePointer, ~ 5% "long" transaction</li>
   *   <li>invalid = 100 randomly distributed, 0..1M</li>
   *   <li>committing = one each, (readPointer + 1)..(readPointer + 100)</li>
   *   <li>committed = one each, (readPointer - 1000)..readPointer</li>
   * </ul>
   * @return a new snapshot of transaction state.
   */
  private TransactionSnapshot createRandomSnapshot() {
    // limit readPointer to a reasonable range, but make it > 1M so we can assign enough keys below
    long readPointer = (Math.abs(random.nextLong()) % 1000000L) + 1000000L;
    long writePointer = readPointer + 1000L;

    // generate in progress -- assume last 500 write pointer values
    NavigableMap<Long, TransactionManager.InProgressTx> inProgress = Maps.newTreeMap();
    long startPointer = writePointer - 500L;
    for (int i = 0; i < 500; i++) {
      long currentTime = System.currentTimeMillis();
      // make some "long" transactions
      if (i % 20 == 0) {
        inProgress.put(startPointer + i,
                       new TransactionManager.InProgressTx(startPointer - 1, currentTime + TimeUnit.DAYS.toSeconds(1),
                                                           TransactionType.LONG));
      } else {
        inProgress.put(startPointer + i,
                       new TransactionManager.InProgressTx(startPointer - 1, currentTime + 300000L, 
                                                           TransactionType.SHORT));
      }
    }

    // make 100 random invalid IDs
    LongArrayList invalid = new LongArrayList();
    for (int i = 0; i < 100; i++) {
      invalid.add(Math.abs(random.nextLong()) % 1000000L);
    }

    // make 100 committing entries, 10 keys each
    Map<Long, Set<ChangeId>> committing = Maps.newHashMap();
    for (int i = 0; i < 100; i++) {
      committing.put(readPointer + i, generateChangeSet(10));
    }

    // make 1000 committed entries, 10 keys each
    long startCommitted = readPointer - 1000L;
    NavigableMap<Long, Set<ChangeId>> committed = Maps.newTreeMap();
    for (int i = 0; i < 1000; i++) {
      committed.put(startCommitted + i, generateChangeSet(10));
    }

    return new TransactionSnapshot(System.currentTimeMillis(), readPointer, writePointer,
                                   invalid, inProgress, committing, committed);
  }

  private Set<ChangeId> generateChangeSet(int numEntries) {
    Set<ChangeId> changes = Sets.newHashSet();
    for (int i = 0; i < numEntries; i++) {
      byte[] bytes = new byte[8];
      random.nextBytes(bytes);
      changes.add(new ChangeId(bytes));
    }
    return changes;
  }
}
