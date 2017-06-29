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

package org.apache.tephra;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Inject;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.manager.InvalidTxList;
import org.apache.tephra.metrics.DefaultMetricsCollector;
import org.apache.tephra.metrics.MetricsCollector;
import org.apache.tephra.persist.NoOpTransactionStateStorage;
import org.apache.tephra.persist.TransactionEdit;
import org.apache.tephra.persist.TransactionLog;
import org.apache.tephra.persist.TransactionLogReader;
import org.apache.tephra.persist.TransactionSnapshot;
import org.apache.tephra.persist.TransactionStateStorage;
import org.apache.tephra.snapshot.SnapshotCodecProvider;
import org.apache.tephra.util.TxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
public class TransactionManager extends AbstractService {
  // todo: optimize heavily

  private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

  // poll every 1 second to check whether a snapshot is needed
  private static final long SNAPSHOT_POLL_INTERVAL = 1000L;

  //poll every 10 second to emit metrics
  private static final long METRICS_POLL_INTERVAL = 10000L;

  //Client id that is used if a client doesn't provide one while starting a transaction.
  private static final String DEFAULT_CLIENTID = "unknown";

  // Transactions that are in progress, with their info.
  private final NavigableMap<Long, InProgressTx> inProgress = new ConcurrentSkipListMap<>();

  // the list of transactions that are invalid (not properly committed/aborted, or timed out)
  private final InvalidTxList invalidTxList = new InvalidTxList();

  // todo: use moving array instead (use Long2ObjectMap<byte[]> in fastutil)
  // todo: should this be consolidated with inProgress?
  // commit time next writePointer -> changes made by this tx
  private final NavigableMap<Long, Set<ChangeId>> committedChangeSets = new ConcurrentSkipListMap<>();
  // not committed yet
  private final Map<Long, Set<ChangeId>> committingChangeSets = Maps.newConcurrentMap();

  private long readPointer;
  private long lastWritePointer;
  private MetricsCollector txMetricsCollector;

  private final TransactionStateStorage persistor;

  private final int cleanupInterval;
  private final int defaultTimeout;
  private final int defaultLongTimeout;
  private final int maxTimeout;
  private DaemonThreadExecutor cleanupThread = null;

  private volatile TransactionLog currentLog;

  // timestamp of the last completed snapshot
  private long lastSnapshotTime;
  // frequency in millis to perform snapshots
  private final long snapshotFrequencyInSeconds;
  // number of most recent snapshots to retain
  private final int snapshotRetainCount;
  private DaemonThreadExecutor snapshotThread;
  private DaemonThreadExecutor metricsThread;

  // lock guarding change of the current transaction log
  private final ReentrantReadWriteLock logLock = new ReentrantReadWriteLock();
  private final Lock logReadLock = logLock.readLock();
  private final Lock logWriteLock = logLock.writeLock();

  // fudge factor (in milliseconds) used when interpreting transactions as LONG based on expiration
  // TODO: REMOVE WITH txnBackwardsCompatCheck()
  private final long longTimeoutTolerance;

  public TransactionManager(Configuration config) {
    this(config, new NoOpTransactionStateStorage(new SnapshotCodecProvider(config)), new DefaultMetricsCollector());
  }

  @Inject
  public TransactionManager(Configuration conf, @Nonnull TransactionStateStorage persistor,
                            MetricsCollector txMetricsCollector) {
    this.persistor = persistor;
    cleanupInterval = conf.getInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL,
                                  TxConstants.Manager.DEFAULT_TX_CLEANUP_INTERVAL);
    maxTimeout = conf.getInt(TxConstants.Manager.CFG_TX_MAX_TIMEOUT,
                             TxConstants.Manager.DEFAULT_TX_MAX_TIMEOUT);
    defaultTimeout = conf.getInt(TxConstants.Manager.CFG_TX_TIMEOUT,
                                 TxConstants.Manager.DEFAULT_TX_TIMEOUT);
    defaultLongTimeout = conf.getInt(TxConstants.Manager.CFG_TX_LONG_TIMEOUT,
                                 TxConstants.Manager.DEFAULT_TX_LONG_TIMEOUT);
    snapshotFrequencyInSeconds = conf.getLong(TxConstants.Manager.CFG_TX_SNAPSHOT_INTERVAL,
                                              TxConstants.Manager.DEFAULT_TX_SNAPSHOT_INTERVAL);
    // must always keep at least 1 snapshot
    snapshotRetainCount = Math.max(conf.getInt(TxConstants.Manager.CFG_TX_SNAPSHOT_RETAIN,
                                               TxConstants.Manager.DEFAULT_TX_SNAPSHOT_RETAIN), 1);

    // intentionally not using a constant, as this config should not be exposed
    // TODO: REMOVE WITH txnBackwardsCompatCheck()
    longTimeoutTolerance = conf.getLong("data.tx.long.timeout.tolerance", 10000);

    //
    this.txMetricsCollector = txMetricsCollector;
    this.txMetricsCollector.configure(conf);
    clear();
  }

  private void clear() {
    invalidTxList.clear();
    inProgress.clear();
    committedChangeSets.clear();
    committingChangeSets.clear();
    lastWritePointer = 0;
    readPointer = 0;
    lastSnapshotTime = 0;
  }

  private boolean isStopping() {
    return State.STOPPING.equals(state());
  }

  @Override
  public synchronized void doStart() {
    LOG.info("Starting transaction manager.");
    txMetricsCollector.start();
    // start up the persistor
    persistor.startAndWait();
    try {
      persistor.setupStorage();
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    // establish defaults in case there is no persistence
    clear();
    // attempt to recover state from last run
    recoverState();
    // start the periodic cleanup thread
    startCleanupThread();
    startSnapshotThread();
    startMetricsThread();
    // initialize the WAL if we did not force a snapshot in recoverState()
    initLog();
    // initialize next write pointer if needed
    if (lastWritePointer == 0) {
      lastWritePointer = getNextWritePointer();
      readPointer = lastWritePointer;
    }

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

  // Emits Transaction Data structures size as metrics
  private void startMetricsThread() {
    LOG.info("Starting periodic Metrics Emitter thread, frequency = " + METRICS_POLL_INTERVAL);
    this.metricsThread = new DaemonThreadExecutor("tx-metrics") {
      @Override
      public void doRun() {
        txMetricsCollector.gauge("committing.size", committingChangeSets.size());
        txMetricsCollector.gauge("committed.size", committedChangeSets.size());
        txMetricsCollector.gauge("inprogress.size", inProgress.size());
        txMetricsCollector.gauge("invalid.size", getInvalidSize());
      }

      @Override
      protected void onShutdown() {
        // perform a final metrics emit
        txMetricsCollector.gauge("committing.size", committingChangeSets.size());
        txMetricsCollector.gauge("committed.size", committedChangeSets.size());
        txMetricsCollector.gauge("inprogress.size", inProgress.size());
        txMetricsCollector.gauge("invalid.size", getInvalidSize());
      }

      @Override
      public long getSleepMillis() {
        return METRICS_POLL_INTERVAL;
      }
    };
    metricsThread.start();
  }

  private void cleanupTimedOutTransactions() {
    List<TransactionEdit> invalidEdits = null;
    logReadLock.lock();
    try {
      synchronized (this) {
        if (!isRunning()) {
          return;
        }
        long currentTime = System.currentTimeMillis();
        Map<Long, InProgressType> timedOut = Maps.newHashMap();
        for (Map.Entry<Long, InProgressTx> tx : inProgress.entrySet()) {
          InProgressTx inProgressTx = tx.getValue();
          long expiration = inProgressTx.getExpiration();
          if (expiration >= 0L && currentTime > expiration) {
            // timed out, remember tx id (can't remove while iterating over entries)
            timedOut.put(tx.getKey(), inProgressTx.getType());
            LOG.info("Tx invalid list: added tx {} belonging to client '{}' because of timeout.",
                     tx.getKey(), inProgressTx.getClientId());
          } else if (expiration < 0) {
            LOG.warn("Transaction {} has negative expiration time {}. Likely cause is the transaction was not " +
                       "migrated correctly, this transaction will be expired immediately", tx.getKey(), expiration);
            timedOut.put(tx.getKey(), InProgressType.LONG);
          }
        }
        if (!timedOut.isEmpty()) {
          invalidEdits = Lists.newArrayListWithCapacity(timedOut.size());
          invalidTxList.addAll(timedOut.keySet());
          for (Map.Entry<Long, InProgressType> tx : timedOut.entrySet()) {
            inProgress.remove(tx.getKey());
            // checkpoints never go into the committing change sets or the edits
            if (!InProgressType.CHECKPOINT.equals(tx.getValue())) {
              committingChangeSets.remove(tx.getKey());
              invalidEdits.add(TransactionEdit.createInvalid(tx.getKey()));
            }
          }

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

  public synchronized TransactionSnapshot getSnapshot() throws IOException {
    TransactionSnapshot snapshot = null;
    if (!isRunning() && !isStopping()) {
      return null;
    }

    long now = System.currentTimeMillis();
    // avoid duplicate snapshots at same timestamp
    if (now == lastSnapshotTime || (currentLog != null && now == currentLog.getTimestamp())) {
      try {
        TimeUnit.MILLISECONDS.sleep(1);
      } catch (InterruptedException ie) { }
    }
    // copy in memory state
    snapshot = getCurrentState();

    LOG.debug("Starting snapshot of transaction state with timestamp {}", snapshot.getTimestamp());
    LOG.debug("Returning snapshot of state: " + snapshot);
    return snapshot;
  }

  /**
   * Take a snapshot of the transaction state and serialize it into the given output stream.
   * @return whether a snapshot was taken.
   */
  public boolean takeSnapshot(OutputStream out) throws IOException {
    TransactionSnapshot snapshot = getSnapshot();
    if (snapshot != null) {
      persistor.writeSnapshot(out, snapshot);
      return true;
    } else {
      return false;
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
          snapshot = getSnapshot();
          if (snapshot == null && !closing) {
            return;
          }
          if (snapshot != null) {
            snapshotTime = snapshot.getTimestamp();
          }

          // roll WAL
          oldLog = currentLog;
          if (!closing) {
            currentLog = persistor.createLog(snapshot.getTimestamp());
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
      if (snapshot != null) {
        persistor.writeSnapshot(snapshot);
        lastSnapshotTime = snapshotTime;

        // clean any obsoleted snapshots and WALs
        long oldestRetainedTimestamp = persistor.deleteOldSnapshots(snapshotRetainCount);
        persistor.deleteLogsOlderThan(oldestRetainedTimestamp);
      }
    } catch (IOException ioe) {
      abortService("Snapshot (timestamp " + snapshotTime + ") failed due to: " + ioe.getMessage(), ioe);
    }
  }

  public synchronized TransactionSnapshot getCurrentState() {
    return TransactionSnapshot.copyFrom(System.currentTimeMillis(), readPointer, lastWritePointer,
                                        invalidTxList, inProgress, committingChangeSets,
                                        committedChangeSets);
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
    Preconditions.checkState(lastWritePointer == 0, "lastWritePointer has been set!");
    Preconditions.checkState(invalidTxList.isEmpty(), "invalid list should be empty!");
    Preconditions.checkState(inProgress.isEmpty(), "inProgress map should be empty!");
    Preconditions.checkState(committingChangeSets.isEmpty(), "committingChangeSets should be empty!");
    Preconditions.checkState(committedChangeSets.isEmpty(), "committedChangeSets should be empty!");
    LOG.info("Restoring snapshot of state: " + snapshot);

    lastSnapshotTime = snapshot.getTimestamp();
    readPointer = snapshot.getReadPointer();
    lastWritePointer = snapshot.getWritePointer();
    invalidTxList.addAll(snapshot.getInvalid());
    inProgress.putAll(txnBackwardsCompatCheck(defaultLongTimeout, longTimeoutTolerance, snapshot.getInProgress()));
    committingChangeSets.putAll(snapshot.getCommittingChangeSets());
    committedChangeSets.putAll(snapshot.getCommittedChangeSets());
  }

  /**
   * Check if in-progress transactions need to be migrated to have expiration time and type, if so do the migration.
   * This is required for backwards compatibility, when long running transactions were represented
   * with expiration time -1. This can be removed when we stop supporting SnapshotCodec version 1.
   */
  public static Map<Long, InProgressTx> txnBackwardsCompatCheck(int defaultLongTimeout, long longTimeoutTolerance,
                                                                Map<Long, InProgressTx> inProgress) {
    for (Map.Entry<Long, InProgressTx> entry : inProgress.entrySet()) {
      long writePointer = entry.getKey();
      long expiration = entry.getValue().getExpiration();
      // LONG transactions will either have a negative expiration or expiration set to the long timeout
      // use a fudge factor on the expiration check, since expiraton is set based on system time, not the write pointer
      if (entry.getValue().getType() == null &&
          (expiration < 0 ||
              (getTxExpirationFromWritePointer(writePointer, defaultLongTimeout) - expiration
                  < longTimeoutTolerance))) {
        // handle null expiration
        long newExpiration = getTxExpirationFromWritePointer(writePointer, defaultLongTimeout);
        InProgressTx compatTx =
          new InProgressTx(entry.getValue().getVisibilityUpperBound(), newExpiration, InProgressType.LONG,
              entry.getValue().getCheckpointWritePointers());
        entry.setValue(compatTx);
      } else if (entry.getValue().getType() == null) {
        InProgressTx compatTx =
          new InProgressTx(entry.getValue().getVisibilityUpperBound(), entry.getValue().getExpiration(),
                           InProgressType.SHORT, entry.getValue().getCheckpointWritePointers());
        entry.setValue(compatTx);
      }
    }
    return inProgress;
  }

  /**
   * Resets the state of the transaction manager.
   */
  public void resetState() {
    this.logWriteLock.lock();
    try {
      // Take a snapshot before resetting the state, for debugging purposes
      doSnapshot(false);
      // Clear the state
      clear();
      // Take another snapshot: if no snapshot is taken after clearing the state
      // and the manager is restarted, we will recover from the snapshot taken
      // before resetting the state, which would be really bad
      // This call will also init a new WAL
      doSnapshot(false);
    } catch (IOException e) {
      LOG.error("Snapshot failed when resetting state!", e);
      e.printStackTrace();
    } finally {
      this.logWriteLock.unlock();
    }
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
              long expiration = edit.getExpiration();
              TransactionType type = edit.getType();
              // Check if transaction needs to be migrated to have expiration and type. Previous version of 
              // long running transactions were represented with expiration time as -1.
              // This can be removed when we stop supporting TransactionEditCodecV2.
              if (expiration < 0) {
                expiration = getTxExpirationFromWritePointer(edit.getWritePointer(), defaultLongTimeout);
                type = TransactionType.LONG;
              } else if (type == null) {
                type = TransactionType.SHORT;
              }
              // We don't persist the client id.
              addInProgressAndAdvance(edit.getWritePointer(), edit.getVisibilityUpperBound(), expiration, type, null);
              break;
            case COMMITTING:
              addCommittingChangeSet(edit.getWritePointer(), edit.getChanges());
              break;
            case COMMITTED:
              // TODO: need to reconcile usage of transaction id v/s write pointer TEPHRA-140
              long transactionId = edit.getWritePointer();
              long[] checkpointPointers = edit.getCheckpointPointers();
              long writePointer = checkpointPointers == null || checkpointPointers.length == 0 ?
                transactionId : checkpointPointers[checkpointPointers.length - 1];
              doCommit(transactionId, writePointer, edit.getChanges(),
                       edit.getCommitPointer(), edit.getCanCommit());
              break;
            case INVALID:
              doInvalidate(edit.getWritePointer());
              break;
            case ABORTED:
              type = edit.getType();
              // Check if transaction edit needs to be migrated to have type. Previous versions of
              // ABORTED edits did not contain type.
              // This can be removed when we stop supporting TransactionEditCodecV2.
              if (type == null) {
                InProgressTx inProgressTx = inProgress.get(edit.getWritePointer());
                if (inProgressTx != null) {
                  InProgressType inProgressType = inProgressTx.getType();
                  if (InProgressType.CHECKPOINT.equals(inProgressType)) {
                    // this should never happen, because checkpoints never go into the log edits;
                    LOG.debug("Ignoring ABORTED edit for a checkpoint transaction {}", edit.getWritePointer());
                    break;
                  }
                  if (inProgressType != null) {
                    type = inProgressType.getTransactionType();
                  }
                } else {
                  // If transaction is not in-progress, then it has either been already aborted or invalidated.
                  // We cannot determine the transaction's state based on current information, to be safe invalidate it.
                  LOG.warn("Invalidating transaction {} as it's type cannot be determined during replay",
                           edit.getWritePointer());
                  doInvalidate(edit.getWritePointer());
                  break;
                }
              }
              doAbort(edit.getWritePointer(), edit.getCheckpointPointers(), type);
              break;
            case TRUNCATE_INVALID_TX:
              if (edit.getTruncateInvalidTxTime() != 0) {
                doTruncateInvalidTxBefore(edit.getTruncateInvalidTxTime());
              } else {
                doTruncateInvalidTx(edit.getTruncateInvalidTx());
              }
              break;
            case CHECKPOINT:
              doCheckpoint(edit.getWritePointer(), edit.getParentWritePointer());
              break;
            default:
              // unknown type!
              throw new IllegalArgumentException("Invalid state for WAL entry: " + edit.getState());
          }
        }
      } catch (IOException ioe) {
        throw Throwables.propagate(ioe);
      } catch (InvalidTruncateTimeException e) {
        throw Throwables.propagate(e);
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
    if (metricsThread != null) {
      metricsThread.shutdown();
      try {
        metricsThread.join(30000L);
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
    txMetricsCollector.stop();
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
   * Start a short transaction with a client id and default timeout.
   * @param clientId id of the client requesting a transaction.
   */
  public Transaction startShort(String clientId) {
    return startShort(clientId, defaultTimeout);
  }

  /**
   * Start a short transaction with a given timeout.
   * @param timeoutInSeconds the time out period in seconds.
   */
  public Transaction startShort(int timeoutInSeconds) {
    return startShort(DEFAULT_CLIENTID, timeoutInSeconds);
  }

  /**
   * Start a short transaction with a given timeout.
   * @param clientId id of the client requesting a transaction.
   * @param timeoutInSeconds the time out period in seconds.
   */
  public Transaction startShort(String clientId, int timeoutInSeconds) {
    Preconditions.checkArgument(clientId != null, "clientId must not be null");
    Preconditions.checkArgument(timeoutInSeconds > 0,
                                "timeout must be positive but is %s seconds", timeoutInSeconds);
    Preconditions.checkArgument(timeoutInSeconds <= maxTimeout,
                                "timeout must not exceed %s seconds but is %s seconds", maxTimeout, timeoutInSeconds);
    txMetricsCollector.rate("start.short");
    Stopwatch timer = new Stopwatch().start();
    long expiration = getTxExpiration(timeoutInSeconds);
    Transaction tx = startTx(expiration, TransactionType.SHORT, clientId);
    txMetricsCollector.histogram("start.short.latency", (int) timer.elapsedMillis());
    return tx;
  }
  
  private static long getTxExpiration(long timeoutInSeconds) {
    long currentTime = System.currentTimeMillis();
    return currentTime + TimeUnit.SECONDS.toMillis(timeoutInSeconds);
  }

  public static long getTxExpirationFromWritePointer(long writePointer, long timeoutInSeconds) {
    return writePointer / TxConstants.MAX_TX_PER_MS + TimeUnit.SECONDS.toMillis(timeoutInSeconds);
  }

  private long getNextWritePointer() {
    // We want to align tx ids with current time. We assume that tx ids are sequential, but not less than
    // System.currentTimeMillis() * MAX_TX_PER_MS.
    return Math.max(lastWritePointer + 1, System.currentTimeMillis() * TxConstants.MAX_TX_PER_MS);
  }

  /**
   * Start a long transaction. Long transactions and do not participate in conflict detection. Also, aborting a long
   * transaction moves it to the invalid list because we assume that its writes cannot be rolled back.
   */
  public Transaction startLong() {
    return startLong(DEFAULT_CLIENTID);
  }

  /**
   * Starts a long transaction with a client id.
   */
  public Transaction startLong(String clientId) {
    Preconditions.checkArgument(clientId != null, "clientId must not be null");
    txMetricsCollector.rate("start.long");
    Stopwatch timer = new Stopwatch().start();
    long expiration = getTxExpiration(defaultLongTimeout);
    Transaction tx = startTx(expiration, TransactionType.LONG, clientId);
    txMetricsCollector.histogram("start.long.latency", (int) timer.elapsedMillis());
    return tx;
  }

  private Transaction startTx(long expiration, TransactionType type, @Nullable String clientId) {
    Transaction tx = null;
    long txid;
    // guard against changes to the transaction log while processing
    this.logReadLock.lock();
    try {
      synchronized (this) {
        ensureAvailable();
        txid = getNextWritePointer();
        tx = createTransaction(txid, type);
        addInProgressAndAdvance(tx.getTransactionId(), tx.getVisibilityUpperBound(), expiration, type, clientId);
      }
      // appending to WAL out of global lock for concurrent performance
      // we should still be able to arrive at the same state even if log entries are out of order
      appendToLog(TransactionEdit.createStarted(tx.getTransactionId(), tx.getVisibilityUpperBound(), expiration, type));
    } finally {
      this.logReadLock.unlock();
    }
    return tx;
  }

  private void addInProgressAndAdvance(long writePointer, long visibilityUpperBound,
                                       long expiration, TransactionType type, @Nullable String clientId) {
    addInProgressAndAdvance(writePointer, visibilityUpperBound, expiration, InProgressType.of(type), clientId);
  }

  private void addInProgressAndAdvance(long writePointer, long visibilityUpperBound,
                                       long expiration, InProgressType type, @Nullable String clientId) {
    inProgress.put(writePointer, new InProgressTx(clientId, visibilityUpperBound, expiration, type));
    advanceWritePointer(writePointer);
  }

  private void advanceWritePointer(long writePointer) {
    // don't move the write pointer back if we have out of order transaction log entries
    if (writePointer > lastWritePointer) {
      lastWritePointer = writePointer;
    }
  }

  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) throws TransactionNotInProgressException {
    txMetricsCollector.rate("canCommit");
    Stopwatch timer = new Stopwatch().start();
    if (inProgress.get(tx.getTransactionId()) == null) {
      synchronized (this) {
        // invalid transaction, either this has timed out and moved to invalid, or something else is wrong.
        if (invalidTxList.contains(tx.getTransactionId())) {
          throw new TransactionNotInProgressException(
            String.format(
              "canCommit() is called for transaction %d that is not in progress (it is known to be invalid)",
              tx.getTransactionId()));
        } else {
          throw new TransactionNotInProgressException(
            String.format("canCommit() is called for transaction %d that is not in progress", tx.getTransactionId()));
        }
      }
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
        addCommittingChangeSet(tx.getTransactionId(), set);
      }
      appendToLog(TransactionEdit.createCommitting(tx.getTransactionId(), set));
    } finally {
      this.logReadLock.unlock();
    }
    txMetricsCollector.histogram("canCommit.latency", (int) timer.elapsedMillis());
    return true;
  }

  private void addCommittingChangeSet(long writePointer, Set<ChangeId> changes) {
    committingChangeSets.put(writePointer, changes);
  }

  public boolean commit(Transaction tx) throws TransactionNotInProgressException {
    txMetricsCollector.rate("commit");
    Stopwatch timer = new Stopwatch().start();
    Set<ChangeId> changeSet = null;
    boolean addToCommitted = true;
    long commitPointer;
    // guard against changes to the transaction log while processing
    this.logReadLock.lock();
    try {
      synchronized (this) {
        ensureAvailable();
        // we record commits at the first not-yet assigned transaction id to simplify clearing out change sets that
        // are no longer visible by any in-progress transactions
        commitPointer = lastWritePointer + 1;
        if (inProgress.get(tx.getTransactionId()) == null) {
          // invalid transaction, either this has timed out and moved to invalid, or something else is wrong.
          if (invalidTxList.contains(tx.getTransactionId())) {
            throw new TransactionNotInProgressException(
              String.format("canCommit() is called for transaction %d that is not in progress " +
                              "(it is known to be invalid)", tx.getTransactionId()));
          } else {
            throw new TransactionNotInProgressException(
              String.format("canCommit() is called for transaction %d that is not in progress", tx.getTransactionId()));
          }
        }

        // these should be atomic
        // NOTE: whether we succeed or not we don't need to keep changes in committing state: same tx cannot
        //       be attempted to commit twice
        changeSet = committingChangeSets.remove(tx.getTransactionId());

        if (changeSet != null) {
          // double-checking if there are conflicts: someone may have committed since canCommit check
          if (hasConflicts(tx, changeSet)) {
            return false;
          }
        } else {
          // no changes
          addToCommitted = false;
        }
        doCommit(tx.getTransactionId(), tx.getWritePointer(), changeSet, commitPointer, addToCommitted);
      }
      appendToLog(TransactionEdit.createCommitted(tx.getTransactionId(), changeSet, commitPointer, addToCommitted));
    } finally {
      this.logReadLock.unlock();
    }
    txMetricsCollector.histogram("commit.latency", (int) timer.elapsedMillis());
    return true;
  }

  private void doCommit(long transactionId, long writePointer, Set<ChangeId> changes, long commitPointer,
                        boolean addToCommitted) {
    // In case this method is called when loading a previous WAL, we need to remove the tx from these sets
    committingChangeSets.remove(transactionId);
    if (addToCommitted && !changes.isEmpty()) {
      // No need to add empty changes to the committed change sets, they will never trigger any conflict

      // Record the committed change set with the next writePointer as the commit time.
      // NOTE: we use current next writePointer as key for the map, hence we may have multiple txs changesets to be
      //       stored under one key
      Set<ChangeId> changeIds = committedChangeSets.get(commitPointer);
      if (changeIds != null) {
        // NOTE: we modify the new set to prevent concurrent modification exception, as other threads (e.g. in
        // canCommit) use it unguarded
        changes.addAll(changeIds);
      }
      committedChangeSets.put(commitPointer, changes);
    }
    // remove from in-progress set, so that it does not get excluded in the future
    InProgressTx previous = inProgress.remove(transactionId);
    if (previous == null) {
      // tx was not in progress! perhaps it timed out and is invalid? try to remove it there.
      if (invalidTxList.remove(transactionId)) {
        LOG.info("Tx invalid list: removed committed tx {}", transactionId);
      }
    } else {
      LongArrayList checkpointPointers = previous.getCheckpointWritePointers();
      if (!checkpointPointers.isEmpty()) {
        // adjust the write pointer to be the last checkpoint of the tx and remove all checkpoints from inProgress
        writePointer = checkpointPointers.getLong(checkpointPointers.size() - 1);
        inProgress.keySet().removeAll(previous.getCheckpointWritePointers());
      }
    }
    // moving read pointer
    moveReadPointerIfNeeded(writePointer);

    // All committed change sets that are smaller than the earliest started transaction can be removed.
    // here we ignore transactions that have no timeout, they are long-running and don't participate in
    // conflict detection.
    // TODO: for efficiency, can we do this once per-log in replayLogs instead of once per edit?
    committedChangeSets.headMap(TxUtils.getFirstShortInProgress(inProgress)).clear();
  }

  public void abort(Transaction tx) {
    // guard against changes to the transaction log while processing
    txMetricsCollector.rate("abort");
    Stopwatch timer = new Stopwatch().start();
    this.logReadLock.lock();
    try {
      synchronized (this) {
        ensureAvailable();
        doAbort(tx.getTransactionId(), tx.getCheckpointWritePointers(), tx.getType());
      }
      appendToLog(TransactionEdit.createAborted(tx.getTransactionId(), tx.getType(), tx.getCheckpointWritePointers()));
      txMetricsCollector.histogram("abort.latency", (int) timer.elapsedMillis());
    } finally {
      this.logReadLock.unlock();
    }
  }

  private void doAbort(long writePointer, long[] checkpointWritePointers, TransactionType type) {
    committingChangeSets.remove(writePointer);
    
    if (type == TransactionType.LONG) {
      // Long running transactions cannot be aborted as their change sets are not saved, 
      // and hence the changes cannot be rolled back. Invalidate the long running transaction instead.
      doInvalidate(writePointer);
      return;
    }
    
    // makes tx visible (assumes that all operations were rolled back)
    // remove from in-progress set, so that it does not get excluded in the future
    InProgressTx removed = inProgress.remove(writePointer);
    boolean removeInProgressCheckpoints = true;
    if (removed == null) {
      // tx was not in progress! perhaps it timed out and is invalid? try to remove it there.
      if (invalidTxList.remove(writePointer)) {
        // the tx and all its children were invalidated: no need to remove them from inProgress
        removeInProgressCheckpoints = false;
        // remove any invalidated checkpoint pointers
        // this will only be present if the parent write pointer was also invalidated
        if (checkpointWritePointers != null) {
          for (long checkpointWritePointer : checkpointWritePointers) {
            invalidTxList.remove(checkpointWritePointer);
          }
        }
        LOG.info("Tx invalid list: removed aborted tx {}", writePointer);
      }
    }
    if (removeInProgressCheckpoints && checkpointWritePointers != null) {
      for (long checkpointWritePointer : checkpointWritePointers) {
        inProgress.remove(checkpointWritePointer);
      }
    }
    // removed a tx from excludes: must move read pointer
    moveReadPointerIfNeeded(writePointer);
  }

  public boolean invalidate(long tx) {
    // guard against changes to the transaction log while processing
    txMetricsCollector.rate("invalidate");
    Stopwatch timer = new Stopwatch().start();
    this.logReadLock.lock();
    try {
      boolean success;
      synchronized (this) {
        ensureAvailable();
        success = doInvalidate(tx);
      }
      appendToLog(TransactionEdit.createInvalid(tx));
      txMetricsCollector.histogram("invalidate.latency", (int) timer.elapsedMillis());
      return success;
    } finally {
      this.logReadLock.unlock();
    }
  }

  private boolean doInvalidate(long writePointer) {
    Set<ChangeId> previousChangeSet = committingChangeSets.remove(writePointer);
    // remove from in-progress set, so that it does not get excluded in the future
    InProgressTx previous = inProgress.remove(writePointer);

    // This check is to prevent from invalidating committed transactions
    if (previous != null || previousChangeSet != null) {
      // add tx to invalids
      invalidTxList.add(writePointer);
      if (previous == null) {
        LOG.debug("Invalidating tx {} in committing change sets but not in-progress", writePointer);
      } else {
        // invalidate any checkpoint write pointers
        LongArrayList childWritePointers = previous.getCheckpointWritePointers();
        if (!childWritePointers.isEmpty()) {
          invalidTxList.addAll(childWritePointers);
          inProgress.keySet().removeAll(childWritePointers);
        }
      }

      String clientId = DEFAULT_CLIENTID;
      if (previous != null && previous.getClientId() != null) {
        clientId = previous.getClientId();
      }
      LOG.info("Tx invalid list: added tx {} belonging to client '{}' because of invalidate", writePointer, clientId);
      if (previous != null && !previous.isLongRunning()) {
        // tx was short-running: must move read pointer
        moveReadPointerIfNeeded(writePointer);
      }
      return true;
    }
    return false;
  }

  /**
   * Removes the given transaction ids from the invalid list.
   * @param invalidTxIds transaction ids
   * @return true if invalid list got changed, false otherwise
   */
  public boolean truncateInvalidTx(Set<Long> invalidTxIds) {
    // guard against changes to the transaction log while processing
    txMetricsCollector.rate("truncateInvalidTx");
    Stopwatch timer = new Stopwatch().start();
    this.logReadLock.lock();
    try {
      boolean success;
      synchronized (this) {
        ensureAvailable();
        success = doTruncateInvalidTx(invalidTxIds);
      }
      appendToLog(TransactionEdit.createTruncateInvalidTx(invalidTxIds));
      txMetricsCollector.histogram("truncateInvalidTx.latency", (int) timer.elapsedMillis());
      return success;
    } finally {
      this.logReadLock.unlock();
    }
  }

  private boolean doTruncateInvalidTx(Set<Long> toRemove) {
    LOG.info("Removing tx ids {} from invalid list", toRemove);
    return invalidTxList.removeAll(toRemove);
  }

  /**
   * Removes all transaction ids started before the given time from invalid list.
   * @param time time in milliseconds
   * @return true if invalid list got changed, false otherwise
   * @throws InvalidTruncateTimeException if there are any in-progress transactions started before given time
   */
  public boolean truncateInvalidTxBefore(long time) throws InvalidTruncateTimeException {
    // guard against changes to the transaction log while processing
    txMetricsCollector.rate("truncateInvalidTxBefore");
    Stopwatch timer = new Stopwatch().start();
    this.logReadLock.lock();
    try {
      boolean success;
      synchronized (this) {
        ensureAvailable();
        success = doTruncateInvalidTxBefore(time);
      }
      appendToLog(TransactionEdit.createTruncateInvalidTxBefore(time));
      txMetricsCollector.histogram("truncateInvalidTxBefore.latency", (int) timer.elapsedMillis());
      return success;
    } finally {
      this.logReadLock.unlock();
    }
  }
  
  private boolean doTruncateInvalidTxBefore(long time) throws InvalidTruncateTimeException {
    LOG.info("Removing tx ids before {} from invalid list", time);
    long truncateWp = time * TxConstants.MAX_TX_PER_MS;
    // Check if there any in-progress transactions started earlier than truncate time
    if (inProgress.lowerKey(truncateWp) != null) {
      throw new InvalidTruncateTimeException("Transactions started earlier than " + time + " are in-progress");
    }
    
    // Find all invalid transactions earlier than truncateWp
    LongSet toTruncate = new LongArraySet();
    LongIterator it = invalidTxList.toRawList().iterator();
    while (it.hasNext()) {
      long wp = it.nextLong();
      if (wp < truncateWp) {
        toTruncate.add(wp);
      }
    }
    LOG.info("Removing tx ids {} from invalid list", toTruncate);
    return invalidTxList.removeAll(toTruncate);
  }

  public Transaction checkpoint(Transaction originalTx) throws TransactionNotInProgressException {
    txMetricsCollector.rate("checkpoint");
    Stopwatch timer = new Stopwatch().start();

    Transaction checkpointedTx = null;
    long txId = originalTx.getTransactionId();
    long newWritePointer = 0;
    // guard against changes to the transaction log while processing
    this.logReadLock.lock();
    try {
      synchronized (this) {
        ensureAvailable();
        // check that the parent tx is in progress
        InProgressTx parentTx = inProgress.get(txId);
        if (parentTx == null) {
          if (invalidTxList.contains(txId)) {
            throw new TransactionNotInProgressException(
                String.format("Transaction %d is not in progress because it was invalidated", txId));
          } else {
            throw new TransactionNotInProgressException(
                String.format("Transaction %d is not in progress", txId));
          }
        }
        newWritePointer = getNextWritePointer();
        doCheckpoint(newWritePointer, txId);
        // create a new transaction with the same read snapshot, plus the additional checkpoint write pointer
        // the same read snapshot is maintained to
        checkpointedTx = new Transaction(originalTx, newWritePointer,
            parentTx.getCheckpointWritePointers().toLongArray());
      }
      // appending to WAL out of global lock for concurrent performance
      // we should still be able to arrive at the same state even if log entries are out of order
      appendToLog(TransactionEdit.createCheckpoint(newWritePointer, txId));
    } finally {
      this.logReadLock.unlock();
    }
    txMetricsCollector.histogram("checkpoint.latency", (int) timer.elapsedMillis());

    return checkpointedTx;
  }

  private void doCheckpoint(long newWritePointer, long parentWritePointer) {
    InProgressTx existingTx = inProgress.get(parentWritePointer);
    existingTx.addCheckpointWritePointer(newWritePointer);
    addInProgressAndAdvance(newWritePointer, existingTx.getVisibilityUpperBound(), existingTx.getExpiration(),
                            InProgressType.CHECKPOINT, existingTx.getClientId());
  }
  
  // hack for exposing important metric
  public int getExcludedListSize() {
    return getInvalidSize() + inProgress.size();
  }

  /**
   * @return the size of invalid list
   */
  public synchronized int getInvalidSize() {
    return this.invalidTxList.size();
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
      if (changeSet.getKey() > tx.getTransactionId()) {
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
  private Transaction createTransaction(long writePointer, TransactionType type) {
    // For holding the first in progress short transaction Id (with timeout >= 0).
    long firstShortTx = Transaction.NO_TX_IN_PROGRESS;
    LongArrayList inProgressIds = new LongArrayList(inProgress.size());
    for (Map.Entry<Long, InProgressTx> entry : inProgress.entrySet()) {
      long txId = entry.getKey();
      inProgressIds.add(txId);
      if (firstShortTx == Transaction.NO_TX_IN_PROGRESS && !entry.getValue().isLongRunning()) {
        firstShortTx = txId;
      }
    }
    return new Transaction(readPointer, writePointer, invalidTxList.toSortedArray(),
                           inProgressIds.toLongArray(), firstShortTx, type);
  }

  private void appendToLog(TransactionEdit edit) {
    try {
      Stopwatch timer = new Stopwatch().start();
      currentLog.append(edit);
      txMetricsCollector.rate("wal.append.count");
      txMetricsCollector.histogram("wal.append.latency", (int) timer.elapsedMillis());
    } catch (IOException ioe) {
      abortService("Error appending to transaction log", ioe);
    }
  }

  private void appendToLog(List<TransactionEdit> edits) {
    try {
      Stopwatch timer = new Stopwatch().start();
      currentLog.append(edits);
      txMetricsCollector.rate("wal.append.count", edits.size());
      txMetricsCollector.histogram("wal.append.latency", (int) timer.elapsedMillis());
    } catch (IOException ioe) {
      abortService("Error appending to transaction log", ioe);
    }
  }

  /**
   * Called from the tx service every 10 seconds.
   * This hack is needed because current metrics system is not flexible when it comes to adding new metrics.
   */
  public void logStatistics() {
    LOG.info("Transaction Statistics: write pointer = " + lastWritePointer +
               ", invalid = " + getInvalidSize() +
               ", in progress = " + inProgress.size() +
               ", committing = " + committingChangeSets.size() +
               ", committed = " + committedChangeSets.size());
  }

  @SuppressWarnings("unused")
  @VisibleForTesting
  public TransactionStateStorage getTransactionStateStorage() {
    return persistor;
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

  /**
   * Type of in-progress transaction.
   */
  public enum InProgressType {

    /**
     * Short transactions detect conflicts during commit.
     */
    SHORT(TransactionType.SHORT),

    /**
     * Long running transactions do not detect conflicts during commit.
     */
    LONG(TransactionType.LONG),

    /**
     * Check-pointed transactions are recorded as in-progress.
     */
    CHECKPOINT(null);

    private final TransactionType transactionType;

    InProgressType(TransactionType transactionType) {
      this.transactionType = transactionType;
    }

    public static InProgressType of(TransactionType type) {
      switch (type) {
        case SHORT: return SHORT;
        case LONG:  return LONG;
        default: throw new IllegalArgumentException("Unknown TransactionType " + type);
      }
    }

    @Nullable
    public TransactionType getTransactionType() {
      return transactionType;
    }
  }

  /**
   * Represents some of the info on in-progress tx
   */
  public static final class InProgressTx {
    /** the oldest in progress tx at the time of this tx start */
    private final long visibilityUpperBound;
    private final long expiration;
    private final InProgressType type;
    private final LongArrayList checkpointWritePointers;
    // clientId is not part of hashCode computation or equality check since it is not persisted. Once it is persisted
    // and restored, we can include it in the above.
    private final String clientId;

    public InProgressTx(String clientId, long visibilityUpperBound, long expiration, InProgressType type) {
      this(clientId, visibilityUpperBound, expiration, type, new LongArrayList());
    }

    public InProgressTx(long visibilityUpperBound, long expiration, InProgressType type) {
      this(visibilityUpperBound, expiration, type, new LongArrayList());
    }

    public InProgressTx(String clientId, long visibilityUpperBound, long expiration, InProgressType type,
                        LongArrayList checkpointWritePointers) {
      this.visibilityUpperBound = visibilityUpperBound;
      this.expiration = expiration;
      this.type = type;
      this.checkpointWritePointers = checkpointWritePointers;
      this.clientId = clientId;
    }

    public InProgressTx(long visibilityUpperBound, long expiration, InProgressType type,
                        LongArrayList checkpointWritePointers) {
      this.visibilityUpperBound = visibilityUpperBound;
      this.expiration = expiration;
      this.type = type;
      this.checkpointWritePointers = checkpointWritePointers;
      this.clientId = null;
    }

    // For backwards compatibility when long running txns were represented with -1 expiration
    @Deprecated
    public InProgressTx(long visibilityUpperBound, long expiration) {
      this(visibilityUpperBound, expiration, null);
    }

    public long getVisibilityUpperBound() {
      return visibilityUpperBound;
    }

    public long getExpiration() {
      return expiration;
    }

    @Nullable
    public InProgressType getType() {
      return type;
    }

    @Nullable
    public String getClientId() {
      return clientId;
    }

    public boolean isLongRunning() {
      if (type == null) {
        // for backwards compatibility when long running txns were represented with -1 expiration
        return expiration == -1;
      }
      return type == InProgressType.LONG;
    }

    public void addCheckpointWritePointer(long checkpointWritePointer) {
      checkpointWritePointers.add(checkpointWritePointer);
    }

    public LongArrayList getCheckpointWritePointers() {
      return checkpointWritePointers;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || !(o instanceof InProgressTx)) {
        return false;
      }

      if (this == o) {
        return true;
      }

      InProgressTx other = (InProgressTx) o;
      return Objects.equal(visibilityUpperBound, other.getVisibilityUpperBound()) &&
          Objects.equal(expiration, other.getExpiration()) &&
          Objects.equal(type, other.type) &&
          Objects.equal(checkpointWritePointers, other.checkpointWritePointers);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(visibilityUpperBound, expiration, type, checkpointWritePointers);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("visibilityUpperBound", visibilityUpperBound)
          .add("expiration", expiration)
          .add("type", type)
          .add("checkpointWritePointers", checkpointWritePointers)
          .add("clientId", clientId)
          .toString();
    }
  }

}
