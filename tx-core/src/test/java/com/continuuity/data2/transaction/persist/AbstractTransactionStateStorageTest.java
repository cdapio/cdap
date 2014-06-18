package com.continuuity.data2.transaction.persist;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.inmemory.ChangeId;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.metrics.TxMetricsCollector;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.longs.LongArrayList;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Commons tests to run against the {@link TransactionStateStorage} implementations.
 */
public abstract class AbstractTransactionStateStorageTest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractTransactionStateStorageTest.class);
  private static Random random = new Random();

  protected abstract CConfiguration getConfiguration(String testName) throws IOException;

  protected abstract AbstractTransactionStateStorage getStorage(CConfiguration conf);

  @Test
  public void testSnapshotPersistence() throws Exception {
    CConfiguration conf = getConfiguration("testSnapshotPersistence");

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
    CConfiguration conf = getConfiguration("testLogWriteAndRead");

    // create some random entries
    List<TransactionEdit> edits = createRandomEdits(100);
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
    CConfiguration conf = getConfiguration("testTransactionManagerPersistence");
    conf.setInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, 0); // no cleanup thread
    // start snapshot thread, but with long enough interval so we only get snapshots on shutdown
    conf.setInt(TxConstants.Manager.CFG_TX_SNAPSHOT_INTERVAL, 600);

    TransactionStateStorage storage = null;
    TransactionStateStorage storage2 = null;
    TransactionStateStorage storage3 = null;
    try {
      storage = getStorage(conf);
      InMemoryTransactionManager txManager = new InMemoryTransactionManager
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
      txManager = new InMemoryTransactionManager(conf, storage2, new TxMetricsCollector());
      txManager.startAndWait();

      // check that the reloaded state matches the old
      TransactionSnapshot newState = txManager.getCurrentState();
      LOG.info("New state: " + newState);
      assertEquals(origState, newState);

      // commit tx2
      Assert.assertTrue(txManager.commit(tx2));
      // start another transaction, must be greater than tx3
      Transaction tx4 = txManager.startShort();
      Assert.assertTrue(tx4.getWritePointer() > tx3.getWritePointer());
      // tx1 must be visble from tx2, but tx3 and tx4 must not
      Assert.assertTrue(tx2.isVisible(tx1.getWritePointer()));
      Assert.assertFalse(tx2.isVisible(tx3.getWritePointer()));
      Assert.assertFalse(tx2.isVisible(tx4.getWritePointer()));
      // add same change for tx3
      Assert.assertFalse(txManager.canCommit(tx3, Collections.singleton(b)));
      // check visibility with new xaction
      Transaction tx5 = txManager.startShort();
      Assert.assertTrue(tx5.isVisible(tx1.getWritePointer()));
      Assert.assertTrue(tx5.isVisible(tx2.getWritePointer()));
      Assert.assertFalse(tx5.isVisible(tx3.getWritePointer()));
      Assert.assertFalse(tx5.isVisible(tx4.getWritePointer()));
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
      txManager = new InMemoryTransactionManager(conf, storage3, new TxMetricsCollector());
      txManager.startAndWait();

      // verify state again matches (this time should include WAL replay)
      newState = txManager.getCurrentState();
      assertEquals(origState, newState);

      // get a new transaction and verify it is greater
      Transaction txAfter = txManager.startShort();
      Assert.assertTrue(txAfter.getWritePointer() > tx.getWritePointer());
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
    CConfiguration conf = getConfiguration("testCommittedSetClearing");
    conf.setInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, 0); // no cleanup thread
    conf.setInt(TxConstants.Manager.CFG_TX_SNAPSHOT_INTERVAL, 0); // no periodic snapshots

    TransactionStateStorage storage1 = null;
    TransactionStateStorage storage2 = null;
    try {
      storage1 = getStorage(conf);
      InMemoryTransactionManager txManager = new InMemoryTransactionManager
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
      txManager = new InMemoryTransactionManager(conf, storage2, new TxMetricsCollector());
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
    CConfiguration conf = getConfiguration("testOldFileRemoval");
    TransactionStateStorage storage = null;
    try {
      storage = getStorage(conf);
      storage.startAndWait();
      long now = System.currentTimeMillis();
      long writePointer = 1;
      Collection<Long> invalid = Lists.newArrayList();
      NavigableMap<Long, InMemoryTransactionManager.InProgressTx> inprogress = Maps.newTreeMap();
      Map<Long, Set<ChangeId>> committing = Maps.newHashMap();
      Map<Long, Set<ChangeId>> committed = Maps.newHashMap();
      TransactionSnapshot snapshot = new TransactionSnapshot(now, 0, writePointer++, invalid,
                                                             inprogress, committing, committed);
      TransactionEdit dummyEdit = TransactionEdit.createStarted(1, 0, Long.MAX_VALUE, 2);

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
    NavigableMap<Long, InMemoryTransactionManager.InProgressTx> inProgress = Maps.newTreeMap();
    long startPointer = writePointer - 500L;
    for (int i = 0; i < 500; i++) {
      long currentTime = System.currentTimeMillis();
      // make some "long" transactions
      if (i % 20 == 0) {
        inProgress.put(startPointer + i,
                       new InMemoryTransactionManager.InProgressTx(startPointer - 1, -currentTime));
      } else {
        inProgress.put(startPointer + i,
                       new InMemoryTransactionManager.InProgressTx(startPointer - 1, currentTime + 300000L));
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

  /**
   * Generates a number of semi-random {@link com.continuuity.data2.transaction.persist.TransactionEdit} instances.
   * These are just randomly selected from the possible states, so would not necessarily reflect a real-world
   * distribution.
   *
   * @param numEntries how many entries to generate in the returned list.
   * @return a list of randomly generated transaction log edits.
   */
  private List<TransactionEdit> createRandomEdits(int numEntries) {
    List<TransactionEdit> edits = Lists.newArrayListWithCapacity(numEntries);
    for (int i = 0; i < numEntries; i++) {
      TransactionEdit.State nextType = TransactionEdit.State.values()[random.nextInt(6)];
      long writePointer = Math.abs(random.nextLong());
      switch (nextType) {
        case INPROGRESS:
          edits.add(
            TransactionEdit.createStarted(writePointer, writePointer - 1,
                                          System.currentTimeMillis() + 300000L, writePointer + 1));
          break;
        case COMMITTING:
          edits.add(TransactionEdit.createCommitting(writePointer, generateChangeSet(10)));
          break;
        case COMMITTED:
          edits.add(TransactionEdit.createCommitted(writePointer, generateChangeSet(10), writePointer + 1,
                                                    random.nextBoolean()));
          break;
        case INVALID:
          edits.add(TransactionEdit.createInvalid(writePointer));
          break;
        case ABORTED:
          edits.add(TransactionEdit.createAborted(writePointer));
          break;
        case MOVE_WATERMARK:
          edits.add(TransactionEdit.createMoveWatermark(writePointer));
          break;
      }
    }
    return edits;
  }
}
