package com.continuuity.data2.transaction.inmemory;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.persist.NoOpTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionEdit;
import com.continuuity.data2.transaction.persist.TransactionLog;
import com.continuuity.data2.transaction.persist.TransactionLogReader;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Inject;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This is the central place to manage all active transactions in the system.
 *
 * A transaction consists of
 * <ul>
 *   <li>A write pointer: This is the version used for all writes of that transaction.</li>
 *   <li>A read pointer: All reads under the transaction use this as an upper bound for the version.</li>
 *   <li>A set of excluded versions: These are the write versions of other transactions that must be excluded from
 *   reads, because those transactions are still in progress, or they failed but couldn't be properly rolled back.</li>
 * </ul>
 * To use the transaction system, a client must follow this sequence of steps:
 * <ol>
 *   <li>Request a new transaction.</li>
 *   <li>Use the transaction to read and write datasets. Datasets are encouraged to cache the writes of the
 *     transaction in memory, to reduce the cost of rollback in case the transaction fails. </li>
 *   <li>Check whether the transaction has conflicts. For this, the set of change keys are submitted via canCommit(),
 *     and the transaction manager verifies that none of these keys are in conflict with other transactions that
 *     committed since the start of this transaction.</li>
 *   <li>If the transaction has conflicts:
 *   <ol>
 *     <li>Roll back the changes in every dataset that was changed. This can happen in-memory if the
 *       changes were cached.</li>
 *     <li>Abort the transaction to remove it from the active transactions.</li>
 *   </ol>
 *   <li>If the transaction has no conflicts:</li>
 *   <ol>
 *     <li>Persist all datasets changes to storage.</li>
 *     <li>Commit the transaction. This will repeat the conflict detection, because more overlapping transactions
 *       may have committed since the first conflict check.</li>
 *     <li>If the transaction has conflicts:</li>
 *     <ol>
 *       <li>Roll back the changes in every dataset that was changed. This is more expensive because
 *         changes must be undone in persistent storage.</li>
 *       <li>Abort the transaction to remove it from the active transactions.</li>
 *     </ol>
 *   </ol>
 * </ol>
 * Transactions may be short or long-running. A short transaction is started with a timeout, and if it is not
 * committed before that timeout, it is invalidated and excluded from future reads. A long-running transaction has
 * no timeout and will remain active until it is committed or aborted. Long transactions are typically used in
 * map/reduce jobs and can produce enormous amounts of changes. Therefore, long transactions do not participate in
 * conflict detection (they would almost always have conflicts). We also assume that the changes of long transactions
 * are not tracked, and therefore cannot be rolled back. Hence, when a long transaction is aborted, it remains in the
 * list of excluded transactions to make its writes invisible.
 */
// TODO: extract persistence logic from this guy: it is "inMemory" one. Subclass or whatever, but decouple it please
public class InMemoryTransactionManager extends AbstractService {
  // todo: optimize heavily

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTransactionManager.class);

  // How many write versions to claim at a time, by default one million
  // TODO: remove watermark and claim size, with log replay and restoration of the nextWritePointer it shouldn't
  // be needed anymore.
  public static final String CFG_TX_CLAIM_SIZE = "data.tx.claim.size";
  public static final int DEFAULT_TX_CLAIM_SIZE = 1000 * 1000;

  // poll every 1 second to check whether a snapshot is needed
  private static final long SNAPSHOT_POLL_INTERVAL = 1000L;

  private static final long[] NO_INVALID_TX = { };

  // Transactions that are in progress, with their expiration time stamp (negative means no expiration).
  private final NavigableMap<Long, Long> inProgress = new ConcurrentSkipListMap<Long, Long>();

  // the list of transactions that are invalid (not properly committed/aborted, or timed out)
  // TODO: explain usage of two arrays
  private final LongArrayList invalid = new LongArrayList();
  private long[] invalidArray = NO_INVALID_TX;

  // todo: use moving array instead (use Long2ObjectMap<byte[]> in fastutil)
  // todo: should this be consolidated with inProgress?
  // commit time nextWritePointer -> changes made by this tx
  private final NavigableMap<Long, Set<ChangeId>> committedChangeSets =
    new ConcurrentSkipListMap<Long, Set<ChangeId>>();
  // not committed yet
  private final Map<Long, Set<ChangeId>> committingChangeSets = Maps.newConcurrentMap();

  private long readPointer;
  private long nextWritePointer;

  // The watermark is the limit up to which we have claimed all write versions, exclusively.
  // Every time a transaction is created that exceeds (or equals) this limit, a new batch of
  // write versions must be claimed, and the new watermark is saved persistently.
  // If the process restarts after a crash, then the full state has not been persisted, and
  // we don't know the greatest write version that was used. But we know that the last saved
  // watermark is a safe upper bound, and it is safe to use it as the next write version
  // (which will immediately trigger claiming a new batch when that transaction starts).
  private long waterMark;
  private long claimSize = DEFAULT_TX_CLAIM_SIZE;

  private final TransactionStateStorage persistor;

  private final int cleanupInterval;
  private final int defaultTimeout;
  private DaemonThreadExecutor cleanupThread = null;

  private volatile TransactionLog currentLog;

  // timestamp of the last completed snapshot
  private long lastSnapshotTime;
  // frequency in millis to perform snapshots
  private final long snapshotFrequencyInSeconds;
  // number of most recent snapshots to retain
  private final int snapshotRetainCount;
  private DaemonThreadExecutor snapshotThread;
  // lock guarding change of the current transaction log
  private final ReentrantReadWriteLock logLock = new ReentrantReadWriteLock();
  private final Lock logReadLock = logLock.readLock();
  private final Lock logWriteLock = logLock.writeLock();


  /**
   * This constructor should only be used for testing. It uses default configuration and a no-op persistor.
   * If this constructor is used, there is no need to call init().
   */
  public InMemoryTransactionManager() {
    this(CConfiguration.create(), new NoOpTransactionStateStorage());
  }

  @Inject
  public InMemoryTransactionManager(CConfiguration conf, @Nonnull TransactionStateStorage persistor) {
    this.persistor = persistor;
    claimSize = conf.getInt(CFG_TX_CLAIM_SIZE, DEFAULT_TX_CLAIM_SIZE);
    cleanupInterval = conf.getInt(Constants.Transaction.Manager.CFG_TX_CLEANUP_INTERVAL,
                                  Constants.Transaction.Manager.DEFAULT_TX_CLEANUP_INTERVAL);
    defaultTimeout = conf.getInt(Constants.Transaction.Manager.CFG_TX_TIMEOUT,
                                 Constants.Transaction.Manager.DEFAULT_TX_TIMEOUT);
    snapshotFrequencyInSeconds = conf.getLong(Constants.Transaction.Manager.CFG_TX_SNAPSHOT_INTERVAL,
                                              Constants.Transaction.Manager.DEFAULT_TX_SNAPSHOT_INTERVAL);
    // must always keep at least 1 snapshot
    snapshotRetainCount = Math.max(conf.getInt(Constants.Transaction.Manager.CFG_TX_SNAPSHOT_RETAIN,
                                               Constants.Transaction.Manager.DEFAULT_TX_SNAPSHOT_RETAIN), 1);
    clear();
  }

  private void clear() {
    invalid.clear();
    invalidArray = NO_INVALID_TX;
    inProgress.clear();
    committedChangeSets.clear();
    committingChangeSets.clear();
    readPointer = 0;
    nextWritePointer = 1;
    waterMark = 0; // this will trigger a claim at the first start transaction
    lastSnapshotTime = 0;
  }

  @Override
  public synchronized void doStart() {
    LOG.info("Starting transaction manager.");
    // start up the persistor
    persistor.startAndWait();
    // establish defaults in case there is no persistence
    clear();
    // attempt to recover state from last run
    recoverState();
    // start the periodic cleanup thread
    startCleanupThread();
    startSnapshotThread();
    // initialize the WAL if we did not force a snapshot in recoverState()
    initLog();

    notifyStarted();
  }

  private void initLog() {
    if (currentLog == null) {
      try {
        currentLog = persistor.createLog(System.currentTimeMillis());
      } catch (IOException ioe) {
        throw Throwables.propagate(ioe);
      }
    }
  }

  private void startCleanupThread() {
    if (cleanupInterval <= 0 || defaultTimeout <= 0) {
      return;
    }
    LOG.info("Starting periodic timed-out transaction cleanup every " + cleanupInterval +
               " seconds with default timeout of " + defaultTimeout + " seconds.");
    this.cleanupThread = new DaemonThreadExecutor("tx-clean-timeout") {
      @Override
      public void doRun() {
        cleanupTimedOutTransactions();
      }

      @Override
      public long getSleepMillis() {
        return cleanupInterval * 1000;
      }
    };
    cleanupThread.start();
  }

  private void startSnapshotThread() {
    if (snapshotFrequencyInSeconds > 0) {
      LOG.info("Starting periodic snapshot thread, frequency = " + snapshotFrequencyInSeconds +
          " seconds, location = " + persistor.getLocation());
      this.snapshotThread = new DaemonThreadExecutor("tx-snapshot") {
        @Override
        public void doRun() {
          long currentTime = System.currentTimeMillis();
          if (lastSnapshotTime < (currentTime - snapshotFrequencyInSeconds * 1000)) {
            try {
              doSnapshot(false);
            } catch (IOException ioe) {
              LOG.error("Periodic snapshot failed!", ioe);
            }
          }
        }

        @Override
        protected void onShutdown() {
          // perform a final snapshot
          try {
            LOG.info("Writing final snapshot prior to shutdown");
            doSnapshot(true);
          } catch (IOException ioe) {
            LOG.error("Failed performing final snapshot on shutdown", ioe);
          }
        }

        @Override
        public long getSleepMillis() {
          return SNAPSHOT_POLL_INTERVAL;
        }
      };
      snapshotThread.start();
    }
  }

  private void cleanupTimedOutTransactions() {
    List<TransactionEdit> invalidEdits = null;
    this.logReadLock.lock();
    try {
      synchronized (this) {
        if (!isRunning()) {
          return;
        }

        long currentTime = System.currentTimeMillis();
        List<Long> timedOut = Lists.newArrayList();
        for (Map.Entry<Long, Long> tx : inProgress.entrySet()) {
          long expiration = tx.getValue();
          if (expiration >= 0L && currentTime > expiration) {
            // timed out, remember tx id (can't remove while iterating over entries)
            timedOut.add(tx.getKey());
          }
        }
        if (!timedOut.isEmpty()) {
          invalidEdits = Lists.newArrayListWithCapacity(timedOut.size());
          for (Long id : timedOut) {
            invalidEdits.add(TransactionEdit.createInvalid(id));
          }
          invalid.addAll(timedOut);
          for (long tx : timedOut) {
            committingChangeSets.remove(tx);
            inProgress.remove(tx);
          }
          // todo: find a more efficient way to keep this sorted. Could it just be an array?
          Collections.sort(invalid);
          invalidArray = invalid.toLongArray();
          LOG.info("Invalidated {} transactions due to timeout.", timedOut.size());
        }
      }
      if (invalidEdits != null) {
          appendToLog(invalidEdits);
      }
    } finally {
      this.logReadLock.unlock();
    }
  }

  private void doSnapshot(boolean closing) throws IOException {
    long snapshotTime = 0L;
    TransactionSnapshot snapshot = null;
    TransactionLog oldLog = null;
    try {
      this.logWriteLock.lock();
      try {
        synchronized (this) {
          if (!isRunning() && !closing) {
            return;
          }

          long now = System.currentTimeMillis();
          // avoid duplicate snapshots at same timestamp
          // this should be safe because doSnapshot is only called from a single thread
          if (now == lastSnapshotTime || (currentLog != null && now == currentLog.getTimestamp())) {
            try {
              TimeUnit.MILLISECONDS.sleep(1);
            } catch (InterruptedException ie) {}
          }
          // copy in memory state
          snapshot = getCurrentState();
          snapshotTime = snapshot.getTimestamp();
          LOG.info("Starting snapshot of transaction state with timestamp {}", snapshotTime);
          LOG.info("Saving snapshot of state: " + snapshot);

          // roll WAL
          oldLog = currentLog;
          if (!closing) {
            currentLog = persistor.createLog(snapshotTime);
          }
        }
        // there may not be an existing log on startup
        if (oldLog != null) {
          oldLog.close();
        }
      } finally {
        this.logWriteLock.unlock();
      }

      // save snapshot
      persistor.writeSnapshot(snapshot);
      lastSnapshotTime = snapshotTime;

      // clean any obsoleted snapshots and WALs
      long oldestRetainedTimestamp = persistor.deleteOldSnapshots(snapshotRetainCount);
      persistor.deleteLogsOlderThan(oldestRetainedTimestamp);
    } catch (IOException ioe) {
      abortService("Snapshot (timestamp " + snapshotTime + ") failed due to: " + ioe.getMessage(), ioe);
    }
  }

  public synchronized TransactionSnapshot getCurrentState() {
    return TransactionSnapshot.copyFrom(System.currentTimeMillis(), readPointer, nextWritePointer, waterMark,
                                            invalid, inProgress, committingChangeSets, committedChangeSets);
  }

  public synchronized void recoverState() {
    try {
      TransactionSnapshot lastSnapshot = persistor.getLatestSnapshot();
      // if we failed before a snapshot could complete, we might not have one to restore
      if (lastSnapshot != null) {
        restoreSnapshot(lastSnapshot);
      }
      // replay any WALs since the last snapshot
      Collection<TransactionLog> logs = persistor.getLogsSince(lastSnapshotTime);
      if (logs != null) {
        replayLogs(logs);
      }
    } catch (IOException e) {
      LOG.error("Unable to read back transaction state:", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Restore the initial in-memory transaction state from a snapshot.
   */
  private void restoreSnapshot(TransactionSnapshot snapshot) {
    LOG.info("Restoring transaction state from snapshot at " + snapshot.getTimestamp());
    Preconditions.checkState(lastSnapshotTime == 0, "lastSnapshotTime has been set!");
    Preconditions.checkState(readPointer == 0, "readPointer has been set!");
    Preconditions.checkState(nextWritePointer == 1, "nextWritePointer has been set!");
    Preconditions.checkState(waterMark == 0, "waterMark has been set!");
    Preconditions.checkState(invalid.isEmpty(), "invalid list should be empty!");
    Preconditions.checkState(inProgress.isEmpty(), "inProgress map should be empty!");
    Preconditions.checkState(committingChangeSets.isEmpty(), "committingChangeSets should be empty!");
    Preconditions.checkState(committedChangeSets.isEmpty(), "committedChangeSets should be empty!");
    LOG.info("Restoring snapshot of state: " + snapshot);

    lastSnapshotTime = snapshot.getTimestamp();
    readPointer = snapshot.getReadPointer();
    nextWritePointer = snapshot.getWritePointer();
    waterMark = snapshot.getWatermark();
    invalid.addAll(snapshot.getInvalid());
    inProgress.putAll(snapshot.getInProgress());
    committingChangeSets.putAll(snapshot.getCommittingChangeSets());
    committedChangeSets.putAll(snapshot.getCommittedChangeSets());
  }

  /**
   * Replay all logged edits from the given transaction logs.
   */
  private void replayLogs(Collection<TransactionLog> logs) {
    for (TransactionLog log : logs) {
      LOG.info("Replaying edits from transaction log " + log.getName());
      int editCnt = 0;
      try {
        TransactionLogReader reader = log.getReader();
        // reader may be null in the case of an empty file
        if (reader == null) {
          continue;
        }
        TransactionEdit edit = null;
        while ((edit = reader.next()) != null) {
          editCnt++;
          switch (edit.getState()) {
            case INPROGRESS:
              addInProgressAndAdvance(edit.getWritePointer(), edit.getExpiration(), edit.getNextWritePointer());
              break;
            case COMMITTING:
              addCommittingChangeSet(edit.getWritePointer(), edit.getChanges());
              break;
            case COMMITTED:
              doCommit(edit.getWritePointer(), edit.getChanges(), edit.getNextWritePointer(), edit.getCanCommit());
              break;
            case INVALID:
              doInvalidate(edit.getWritePointer());
              break;
            case ABORTED:
              doAbort(edit.getWritePointer());
              break;
            case MOVE_WATERMARK:
              waterMark = edit.getWritePointer();
              break;
            default:
              // unknown type!
              throw new IllegalArgumentException("Invalid state for WAL entry: " + edit.getState());
          }
        }
      } catch (IOException ioe) {
        throw Throwables.propagate(ioe);
      }
      LOG.info("Read " + editCnt + " edits from log " + log.getName());
    }
  }

  @Override
  public void doStop() {
    Stopwatch timer = new Stopwatch().start();
    LOG.info("Shutting down gracefully...");
    // signal the cleanup thread to stop
    if (cleanupThread != null) {
      cleanupThread.shutdown();
      try {
        cleanupThread.join(30000L);
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted waiting for cleanup thread to stop");
        Thread.currentThread().interrupt();
      }
    }
    if (snapshotThread != null) {
      // this will trigger a final snapshot on stop
      snapshotThread.shutdown();
      try {
        snapshotThread.join(30000L);
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted waiting for snapshot thread to stop");
        Thread.currentThread().interrupt();
      }
    }

    persistor.stopAndWait();
    timer.stop();
    LOG.info("Took " + timer + " to stop");
    notifyStopped();
  }

  /**
   * Immediately shuts down the service, without going through the normal close process.
   * @param message A message describing the source of the failure.
   * @param error Any exception that caused the failure.
   */
  private void abortService(String message, Throwable error) {
    if (isRunning()) {
      LOG.error("Aborting transaction manager due to: " + message, error);
      notifyFailed(error);
    }
  }

  // not synchronized because it is only called from start() which is synchronized
  private void saveWaterMarkIfNeeded() {
    try {
      if (nextWritePointer >= waterMark) {
        long nextWatermark = waterMark + claimSize;
        this.logReadLock.lock();
        try {
          appendToLog(TransactionEdit.createMoveWatermark(nextWatermark));
        } finally {
          this.logReadLock.unlock();
        }
        waterMark = nextWatermark;
        LOG.debug("Claimed {} write versions, new watermark is {}.", claimSize, waterMark);
      }
    } catch (Exception e) {
      LOG.error("Unable to persist transaction watermark:", e);
      throw Throwables.propagate(e);
    }
  }

  private void ensureAvailable() {
    Preconditions.checkState(isRunning(), "Transaction Manager is not running.");
  }

  /**
   * Start a short transaction with the default timeout.
   */
  public Transaction startShort() {
    return startShort(defaultTimeout);
  }

  /**
   * Start a short transaction with a given timeout.
   * @param timeoutInSeconds the time out period in seconds.
   */
  public Transaction startShort(int timeoutInSeconds) {
    Preconditions.checkArgument(timeoutInSeconds > 0, "timeout must be positive but is %s", timeoutInSeconds);
    long currentTime = System.currentTimeMillis();
    long expiration = currentTime + 1000L * timeoutInSeconds;
    Transaction tx = null;
    // guard against changes to the transaction log while processing
    this.logReadLock.lock();
    try {
      synchronized (this) {
        ensureAvailable();
        saveWaterMarkIfNeeded();
        tx = createTransaction(nextWritePointer);
        addInProgressAndAdvance(tx.getWritePointer(), expiration, nextWritePointer + 1);
      }
      // appending to WAL out of global lock for concurrent performance
      // we should still be able to arrive at the same state even if log entries are out of order
      appendToLog(TransactionEdit.createStarted(tx.getWritePointer(), expiration, nextWritePointer));
    } finally {
      this.logReadLock.unlock();
    }
    return tx;
  }

  /**
   * Start a long transaction. Long transactions and do not participate in conflict detection. Also, aborting a long
   * transaction moves it to the invalid list because we assume that its writes cannot be rolled back.
   */
  public Transaction startLong() {
    long currentTime = System.currentTimeMillis();
    Transaction tx = null;
    // guard against changes to the transaction log while processing
    this.logReadLock.lock();
    try {
      synchronized (this) {
        ensureAvailable();
        saveWaterMarkIfNeeded();
        tx = createTransaction(nextWritePointer);
        addInProgressAndAdvance(tx.getWritePointer(), -currentTime, nextWritePointer + 1);
      }
      appendToLog(TransactionEdit.createStarted(tx.getWritePointer(), -currentTime, nextWritePointer));
    } finally {
      this.logReadLock.unlock();
    }
    return tx;
  }

  private void addInProgressAndAdvance(long writePointer, long expiration, long nextPointer) {
    inProgress.put(writePointer, expiration);
    // don't move the write pointer back if we have out of order transaction log entries
    if (nextPointer > nextWritePointer) {
      nextWritePointer = nextPointer;
    }
  }

  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) {
    if (inProgress.get(tx.getWritePointer()) == null) {
      // invalid transaction, either this has timed out and moved to invalid, or something else is wrong.
      return false;
    }

    Set<ChangeId> set = Sets.newHashSetWithExpectedSize(changeIds.size());
    for (byte[] change : changeIds) {
      set.add(new ChangeId(change));
    }

    if (hasConflicts(tx, set)) {
      return false;
    }
    // guard against changes to the transaction log while processing
    this.logReadLock.lock();
    try {
      synchronized (this) {
        ensureAvailable();
        addCommittingChangeSet(tx.getWritePointer(), set);
      }
      appendToLog(TransactionEdit.createCommitting(tx.getWritePointer(), set));
    } finally {
      this.logReadLock.unlock();
    }
    return true;
  }

  private void addCommittingChangeSet(long writePointer, Set<ChangeId> changes) {
    committingChangeSets.put(writePointer, changes);
  }

  public boolean commit(Transaction tx) {

    Set<ChangeId> changeSet = null;
    boolean canCommit = true;
    // guard against changes to the transaction log while processing
    this.logReadLock.lock();
    try {
      synchronized (this) {
        ensureAvailable();
        if (inProgress.get(tx.getWritePointer()) == null) {
          // invalid transaction, either this has timed out and moved to invalid, or something else is wrong.
          // if it is missing from inProgress, it is also removed from committing
          return false;
        }

        // todo: these should be atomic
        // NOTE: whether we succeed or not we don't need to keep changes in committing state: same tx cannot
        //       be attempted to commit twice
        changeSet = committingChangeSets.get(tx.getWritePointer());

        if (changeSet != null) {
          // double-checking if there are conflicts: someone may have committed since canCommit check
          if (hasConflicts(tx, changeSet)) {
            canCommit = false;
          }
          if (!canCommit) {
            // encountered conflicts
            return false;
          }
        } else {
          // no changes
          canCommit = false;
        }
        doCommit(tx.getWritePointer(), changeSet, nextWritePointer, canCommit);
      }
      appendToLog(TransactionEdit.createCommitted(tx.getWritePointer(), changeSet, nextWritePointer, canCommit));
    } finally {
      this.logReadLock.unlock();
    }

    return true;
  }

  private void doCommit(long writePointer, Set<ChangeId> changes, long commitPointer, boolean addToCommitted) {
    // NOTE: whether we succeed or not we don't need to keep changes in committing state: same tx cannot
    //       be attempted to commit twice
    committingChangeSets.remove(writePointer);

    if (addToCommitted) {
      // Record the committed change set with the nextWritePointer as the commit time.
      // NOTE: we use current next writePointer as key for the map, hence we may have multiple txs changesets to be
      //       stored under one key
      Set<ChangeId> changeIds = committedChangeSets.get(commitPointer);
      if (changeIds != null) {
        // NOTE: we modify the new set to prevent concurrent modification exception, as other threads (e.g. in
        // canCommit) use it unguarded
        changes.addAll(changeIds);
      }

      committedChangeSets.put(nextWritePointer, changes);
    }
    // remove from in-progress set, so that it does not get excluded in the future
    Long previous = inProgress.remove(writePointer);
    if (previous == null) {
      // tx was not in progress! perhaps it timed out and is invalid? try to remove it there.
      if (invalid.rem(writePointer)) {
        invalidArray = invalid.toLongArray();
      }
    }
    // moving read pointer
    moveReadPointerIfNeeded(writePointer);

    // All committed change sets that are smaller than the earliest started transaction can be removed.
    // here we ignore transactions that have no timeout, they are long-running and don't participate in
    // conflict detection.
    // TODO: for efficiency, can we do this once per-log in replayLogs instead of once per edit?
    committedChangeSets.headMap(firstShortInProgress()).clear();
  }

  // find the first non long-running in-progress tx, or Long.MAX if none such exists
  private long firstShortInProgress() {
    for (Map.Entry<Long, Long> tx : inProgress.entrySet()) {
      if (tx.getValue() >= 0) {
        return tx.getKey();
      }
    }
    return Transaction.NO_TX_IN_PROGRESS;
  }

  public void abort(Transaction tx) {
    // guard against changes to the transaction log while processing
    this.logReadLock.lock();
    try {
      synchronized (this) {
        ensureAvailable();
        doAbort(tx.getWritePointer());
      }
      appendToLog(TransactionEdit.createAborted(tx.getWritePointer()));
    } finally {
      this.logReadLock.unlock();
    }
  }

  private void doAbort(long writePointer) {
    committingChangeSets.remove(writePointer);
    // makes tx visible (assumes that all operations were rolled back)
    // remove from in-progress set, so that it does not get excluded in the future
    Long expirationTs = inProgress.remove(writePointer);
    // TODO: this is bad/misleading/not clear logic. We should have special flags/tx attributes instead of it. Refactor!
    if (expirationTs != null && expirationTs < 0) {
      // tx was long-running: it must be moved to invalid because its operations cannot be rolled back
      invalid.add(writePointer);
      // todo: find a more efficient way to keep this sorted. Could it just be an array?
      Collections.sort(invalid);
      invalidArray = invalid.toLongArray();
    } else if (expirationTs == null) {
      // tx was not in progress! perhaps it timed out and is invalid? try to remove it there.
      if (invalid.rem(writePointer)) {
        invalidArray = invalid.toLongArray();
        // removed a tx from excludes: must move read pointer
        moveReadPointerIfNeeded(writePointer);
      }
    } else {
      // removed a tx from excludes: must move read pointer
      moveReadPointerIfNeeded(writePointer);
    }
  }

  public void invalidate(Transaction tx) {
    // guard against changes to the transaction log while processing
    this.logReadLock.lock();
    try {
      synchronized (this) {
        ensureAvailable();
        doInvalidate(tx.getWritePointer());
      }
      appendToLog(TransactionEdit.createInvalid(tx.getWritePointer()));
    } finally {
      this.logReadLock.unlock();
    }
  }

  public void doInvalidate(long writePointer) {
    committingChangeSets.remove(writePointer);
    // add tx to invalids
    invalid.add(writePointer);
    // todo: find a more efficient way to keep this sorted. Could it just be an array?
    Collections.sort(invalid);
    invalidArray = invalid.toLongArray();
    // remove from in-progress set, so that it does not get excluded in the future
    Long previous = inProgress.remove(writePointer);
    if (previous != null && previous >= 0) {
      // tx was short-running: must move read pointer
      moveReadPointerIfNeeded(writePointer);
    }
  }

  // hack for exposing important metric
  public int getExcludedListSize() {
    return invalid.size() + inProgress.size();
  }
  // package visible hack for exposing internals to unit tests
  int getInvalidSize() {
    return this.invalid.size();
  }
  int getCommittedSize() {
    return this.committedChangeSets.size();
  }

  private boolean hasConflicts(Transaction tx, Set<ChangeId> changeIds) {
    if (changeIds.isEmpty()) {
      return false;
    }

    for (Map.Entry<Long, Set<ChangeId>> changeSet : committedChangeSets.entrySet()) {
      // If commit time is greater than tx read-pointer,
      // basically not visible but committed means "tx committed after given tx was started"
      if (changeSet.getKey() > tx.getWritePointer()) {
        if (overlap(changeSet.getValue(), changeIds)) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean overlap(Set<ChangeId> a, Set<ChangeId> b) {
    // iterate over the smaller set, and check for every element in the other set
    if (a.size() > b.size()) {
      for (ChangeId change : b) {
        if (a.contains(change)) {
          return true;
        }
      }
    } else {
      for (ChangeId change : a) {
        if (b.contains(change)) {
          return true;
        }
      }
    }
    return false;
  }

  private void moveReadPointerIfNeeded(long committedWritePointer) {
    if (committedWritePointer > readPointer) {
      readPointer = committedWritePointer;
    }
  }

  /**
   * Creates a new Transaction. This method only get called from start transaction, which is already
   * synchronized.
   */
  private Transaction createTransaction(long writePointer) {
    // For holding the first in progress short transaction Id (with timeout >= 0).
    long firstShortTx = Transaction.NO_TX_IN_PROGRESS;
    long[] array = new long[inProgress.size()];
    int i = 0;
    for (Map.Entry<Long, Long> entry : inProgress.entrySet()) {
      long txId = entry.getKey();
      array[i++] = txId;
      if (firstShortTx == Transaction.NO_TX_IN_PROGRESS && entry.getValue() >= 0) {
        firstShortTx = txId;
      }
    }

    return new Transaction(readPointer, writePointer, invalidArray, array, firstShortTx);
  }

  private void appendToLog(TransactionEdit edit) {
    try {
      currentLog.append(edit);
    } catch (IOException ioe) {
      abortService("Error appending to transaction log", ioe);
    }
  }

  private void appendToLog(List<TransactionEdit> edits) {
    try {
      currentLog.append(edits);
    } catch (IOException ioe) {
      abortService("Error appending to transaction log", ioe);
    }
  }

/*
  private long[] getExcludedListAsArray() {
    // todo: optimize (cache, etc. etc.)
    long[] elements = new long[invalid.size() + inProgress.size()];
    // merge invalid and in progress
    LongListIterator invalidIter;
    Long currentInvalid;
    int currentIndex = 0;
    if (invalid.isEmpty()) {
      invalidIter = null;
      currentInvalid = null;
    } else {
      invalidIter = invalid.iterator();
      currentInvalid = invalidIter.next();
    }
    for (Long tx : inProgress.keySet()) {
      // consumer all invalid transactions <= this in-progress transaction
      if (currentInvalid != null) {
        while (tx >= currentInvalid) {
          elements[currentIndex++] = currentInvalid;
          if (invalidIter.hasNext()) {
            currentInvalid = invalidIter.next();
          } else {
            currentInvalid = null;
            break;
          }
        }
      }
      // consume this transaction
      elements[currentIndex++] = tx;
    }
    if (currentInvalid != null) {
      elements[currentIndex++] = currentInvalid;
      while (invalidIter.hasNext()) {
        elements[currentIndex++] = invalidIter.next();
      }
    }
    return elements;
  }
*/

  /**
   * Called from the tx service every 10 seconds.
   * This hack is needed because current metrics system is not flexible when it comes to adding new metrics.
   */
  public void logStatistics() {
    LOG.info("Transaction Statistics: write pointer = " + nextWritePointer +
               ", watermark = " + waterMark +
               ", invalid = " + invalid.size() +
               ", in progress = " + inProgress.size() +
               ", committing = " + committingChangeSets.size() +
               ", committed = " + committedChangeSets.size());
  }

  private abstract static class DaemonThreadExecutor extends Thread {
    private AtomicBoolean stopped = new AtomicBoolean(false);

    public DaemonThreadExecutor(String name) {
      super(name);
      setDaemon(true);
    }

    public void run() {
      try {
        while (!isInterrupted() && !stopped.get()) {
          doRun();
          synchronized (stopped) {
            stopped.wait(getSleepMillis());
          }
        }
      } catch (InterruptedException ie) {
        LOG.info("Interrupted thread " + getName());
      }
      // perform any final cleanup
      onShutdown();
      LOG.info("Exiting thread " + getName());
    }

    public abstract void doRun();

    protected abstract long getSleepMillis();

    protected void onShutdown() {
    }

    public void shutdown() {
      if (stopped.compareAndSet(false, true)) {
        synchronized (stopped) {
          stopped.notifyAll();
        }
      }
    }
  }
}
