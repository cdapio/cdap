package com.continuuity.data2.transaction.inmemory;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongListIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// todo: optimize heavily
public class InMemoryTransactionManager {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTransactionManager.class);

  // How many write versions to claim at a time, by default one million
  public static final String CFG_TX_CLAIM_SIZE = "data.tx.claim.size";
  public static final int DEFAULT_TX_CLAIM_SIZE = 1000 * 1000;
  // how often to clean up timed out transactions, in seconds, or 0 for no cleanup
  public static final String CFG_TX_CLEANUP_INTERVAL = "data.tx.cleanup.interval";
  public static final int DEFAULT_TX_CLEANUP_INTERVAL = 60; // how often to clean up timed out transactions, in seconds
  // the timeout for a transaction, in seconds. If the transaction is not finished in that time, it is marked invalid
  public static final String CFG_TX_TIMEOUT = "data.tx.timeout";
  public static final int DEFAULT_TX_TIMEOUT = 300;

  private static final int STATE_PERSIST_VERSION = 1;
  private static final String ALL_STATE_TAG = "all";
  private static final String WATERMARK_TAG = "mark";

  // the set of transactions that are in progress, with their expiration time stamp,
  // or with the negative start time to specify no expiration. We remember the start
  // time to allow diagnostics and possible manual cleanup/invalidation (not implemented yet).
  private final NavigableMap<Long, Long> inProgress = Maps.newTreeMap();
  // the list of transactions that are invalid (not properly committed/aborted, or timed out)
  private final LongArrayList invalid = new LongArrayList();
  // todo: use moving array instead (use Long2ObjectMap<byte[]> in fastutil)
  // todo: should this be consolidated with inProgress?
  // commit time nextWritePointer -> changes made by this tx
  private final NavigableMap<Long, Set<byte[]>> committedChangeSets = Maps.newTreeMap();
  // not committed yet
  private final Map<Long, Set<byte[]>> committingChangeSets = Maps.newHashMap();

  private long readPointer;
  private long nextWritePointer;

  private boolean initialized = false;

  // The watermark is the limit up to which we have claimed all write versions, exclusively.
  // Every time a transaction is created that exceeds (or equals) this limit, a new batch of
  // write versions must be claimed, and the new watermark is saved persistently.
  // If the process restarts after a crash, then the full state has not been persisted, and
  // we don't know the greatest write version that was used. But we know that the last saved
  // watermark is a safe upper bound, and it is safe to use it as the next write version
  // (which will immediately trigger claiming a new batch when that transaction starts).
  private long waterMark;
  private long claimSize = DEFAULT_TX_CLAIM_SIZE;

  private final StatePersistor persistor;

  private final int cleanupInterval;
  private final int defaultTimeout;
  private Thread cleanupThread = null;

  /**
   * This constructor should only be used for testing. It uses default configuration and a no-op persistor.
   * If this constructor is used, there is no need to call init().
   */
  public InMemoryTransactionManager() {
    this(CConfiguration.create(), new NoopPersistor());
    persistor.startAndWait();
    initialized = true;
  }

  @Inject
  public InMemoryTransactionManager(CConfiguration conf, @Nonnull StatePersistor persistor) {
    this.persistor = persistor;
    claimSize = conf.getInt(CFG_TX_CLAIM_SIZE, DEFAULT_TX_CLAIM_SIZE);
    cleanupInterval = conf.getInt(CFG_TX_CLEANUP_INTERVAL, DEFAULT_TX_CLEANUP_INTERVAL);
    defaultTimeout = conf.getInt(CFG_TX_TIMEOUT, DEFAULT_TX_TIMEOUT);
    clear();
  }

  private void clear() {
    invalid.clear();
    inProgress.clear();
    committedChangeSets.clear();
    committingChangeSets.clear();
    readPointer = 0;
    nextWritePointer = 1;
    waterMark = 0; // this will trigger a claim at the first start transaction
  }

  // TODO this class should implement Service and this should be start().
  // TODO However, start() is already used to start a transaction, so this would be major refactoring now.
  public synchronized void init() {
    // start up the persistor
    persistor.startAndWait();
    // establish defaults in case there is no persistence
    clear();
    // attempt to recover state from last run
    recoverState();
    // start the periodic cleanup thread
    startCleanupThread();
    initialized = true;
  }

  private void startCleanupThread() {
    if (cleanupInterval <= 0 && defaultTimeout <= 0) {
      return;
    }
    LOG.info("Starting periodic timed-out transaction cleanup every " + cleanupInterval +
               " seconds with default timeout of " + defaultTimeout + " seconds.");
    this.cleanupThread = new Thread("tx-clean-timeout") {
      @Override
      public void run() {
        while (!isInterrupted()) {
          cleanupTimedOutTransactions();
          try {
            TimeUnit.SECONDS.sleep(cleanupInterval);
          } catch (InterruptedException e) {
            break;
          }
        }
      }
    };
    cleanupThread.setDaemon(true);
    cleanupThread.start();
  }

  private synchronized void cleanupTimedOutTransactions() {
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
      invalid.addAll(timedOut);
      for (long tx : timedOut) {
        committingChangeSets.remove(tx);
        inProgress.remove(tx);
      }
      // todo: find a more efficient way to keep this sorted. Could it just be an array?
      Collections.sort(invalid);
      LOG.info("Invalidated {} transactions due to timeout.", timedOut.size());
    }
  }

  public void recoverState() {
    // try to recover persisted state
    try {
      // attempt to restore the full state
      byte[] state = persistor.readBack(ALL_STATE_TAG);
      if (state != null) {
        decodeState(state);
        LOG.info("Restored transaction state successfully ({} bytes).", state.length);
        persistor.delete(ALL_STATE_TAG);
      } else {
        // full state is not there, attempt to restore the watermark
        state = persistor.readBack(WATERMARK_TAG);
        if (state != null) {
          waterMark = Bytes.toLong(state);
          // must have crashed last time... need to claim the next batch of write versions
          waterMark += claimSize;
          readPointer = waterMark - 1;
          nextWritePointer = waterMark; //
          LOG.warn("Recovered transaction watermark successfully, but transaction state may have been lost.");
        } else {
          LOG.info("No persisted transaction state found. Initializing from scratch.");
        }
      }
    } catch (IOException e) {
      LOG.error("Unable to read back transaction state:", e);
      throw Throwables.propagate(e);
    }
  }

  public synchronized void close() {
    // if initialized is false, then the service did not start up properly and the state is most likely corrupt.
    if (initialized) {
      LOG.info("Shutting down gracefully...");
      // signal the cleanup thread to stop
      if (cleanupThread != null) {
        cleanupThread.interrupt();
      }
      byte[] state = encodeState();
      try {
        persistor.persist(ALL_STATE_TAG, state);
        LOG.info("Successfully persisted transaction state ({} bytes).", state.length);
      } catch (IOException e) {
        LOG.error("Unable to persist transaction state (" + state.length + " bytes):", e);
        throw Throwables.propagate(e);
      }
    }
    persistor.stopAndWait();
  }

  // not synchronized because it is only called from start() which is synchronized
  private void saveWaterMarkIfNeeded() {
    try {
      if (nextWritePointer >= waterMark) {
        waterMark += claimSize;
        persistor.persist(WATERMARK_TAG, Bytes.toBytes(waterMark));
        LOG.debug("Claimed {} write versions, new watermark is {}.", claimSize, waterMark);
      }
    } catch (Exception e) {
      LOG.error("Unable to persist transaction watermark:", e);
      throw Throwables.propagate(e);
    }
  }

  private void ensureInitialized() {
    if (!initialized) {
      throw new IllegalStateException("Transaction Manager was not initialized. ");
    }
  }

  /**
   * Start a transaction with the default timeout.
   */
  public Transaction start() {
    return start(defaultTimeout);
  }

  /**
   * Start a transaction with a given timeout. The contract is that transactions that have no timeout are considered
   * long-running transactions and do not participate in conflict detection. Also, aborting a long-running transaction
   * moves it to the invalid list because we assume that its writes cannot be rolled back.
   * @param timeoutInSeconds the time out period in seconds. Null means no timeout.
   */
  public synchronized Transaction start(@Nullable Integer timeoutInSeconds) {
    ensureInitialized();
    saveWaterMarkIfNeeded();
    Transaction tx = new Transaction(readPointer, nextWritePointer, getExcludedListAsArray());
    long currentTime =  System.currentTimeMillis();
    long expiration = timeoutInSeconds == null ? -currentTime : currentTime + 1000L * timeoutInSeconds;
    inProgress.put(nextWritePointer, expiration);
    nextWritePointer++;
    return tx;
  }

  public synchronized boolean canCommit(Transaction tx, Collection<byte[]> changeIds) {
    if (inProgress.get(tx.getWritePointer()) == null) {
      // invalid transaction, either this has timed out and moved to invalid, or something else is wrong.
      inProgress.remove(tx.getWritePointer());
      return false;
    }

    if (hasConflicts(tx, changeIds)) {
      return false;
    }

    // The change set will never get modified. Using a immutable has smaller footprint and could perform better.
    Set<byte[]> set = ImmutableSortedSet.copyOf(Bytes.BYTES_COMPARATOR, changeIds);
    committingChangeSets.put(tx.getWritePointer(), set);

    return true;
  }

  public synchronized boolean commit(Transaction tx) {

    // todo: these should be atomic
    // NOTE: whether we succeed or not we don't need to keep changes in committing state: same tx cannot be attempted to
    //       commit twice
    Set<byte[]> changeSet = committingChangeSets.remove(tx.getWritePointer());

    if (inProgress.get(tx.getWritePointer()) == null) {
      // invalid transaction, either this has timed out and moved to invalid, or something else is wrong.
      return false;
    }

    if (changeSet != null) {
      // double-checking if there are conflicts: someone may have committed since canCommit check
      if (hasConflicts(tx, changeSet)) {
        return false;
      }

      // Record the committed change set with the nextWritePointer as the commit time.
      if (committedChangeSets.containsKey(nextWritePointer)) {
        // todo: can this ever happen?
        committedChangeSets.get(nextWritePointer).addAll(changeSet);
      } else {
        TreeSet<byte[]> committedChangeSet = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
        committedChangeSet.addAll(changeSet);
        committedChangeSets.put(nextWritePointer, committedChangeSet);
      }
    }
    makeVisible(tx);

    // All committed change sets that are smaller than the earliest started transaction can be removed.
    // here we ignore transactions that have no timeout, they are long-running and don't participate in
    // conflict detection.
    committedChangeSets.headMap(firstInProgressWithTimeout()).clear();
    return true;
  }

  // find the first non long-running in-progress tx, or Long.MAX if none such exists
  private long firstInProgressWithTimeout() {
    for (Map.Entry<Long, Long> tx : inProgress.entrySet()) {
      if (tx.getValue() >= 0) {
        return tx.getKey();
      }
    }
    return Long.MAX_VALUE;
  }

  public synchronized boolean abort(Transaction tx) {
    committingChangeSets.remove(tx.getWritePointer());
    // makes tx visible (assumes that all operations were rolled back)
    // remove from in-progress set, so that it does not get excluded in the future
    Long previous = inProgress.remove(tx.getWritePointer());
    if (previous != null && previous < 0) {
      // tx was long-running: it must be moved to invalid because its operations cannot be rolled back
      invalid.add(tx.getWritePointer());
      // todo: find a more efficient way to keep this sorted. Could it just be an array?
      Collections.sort(invalid);
    } else if (previous == null) {
        // tx was not in progress! perhaps it timed out and is invalid? try to remove it there.
        if (invalid.rem(tx.getWritePointer())) {
          // removed a tx from excludes: must move read pointer
          moveReadPointerIfNeeded(tx.getWritePointer());
        }
    } else {
      // removed a tx from excludes: must move read pointer
      moveReadPointerIfNeeded(tx.getWritePointer());
    }
    return true;
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

//  private static boolean hasConflicts(Transaction tx, Collection<byte[]> changeIds) {
//    if (changeIds.isEmpty()) {
//      return false;
//    }
//
//    // Go thru all tx committed after given tx was started and check if any of them has change
//    // conflicting with the given
//    return hasConflicts(tx, changeIds);
//
//    // NOTE: we could try to optimize for some use-cases and also check those being committed for conflicts to
//    //       avoid later the cost of rollback. This is very complex, but the cost of rollback is so high that we
//    //       can go a bit crazy optimizing stuff around it...
//  }

  private boolean hasConflicts(Transaction tx, Collection<byte[]> changeIds) {
    if (changeIds.isEmpty()) {
      return false;
    }

    for (Map.Entry<Long, Set<byte[]>> changeSet : committedChangeSets.entrySet()) {
      // If commit time is greater than tx read-pointer,
      // basically not visible but committed means "tx committed after given tx was started"
      if (changeSet.getKey() > tx.getWritePointer()) {
        if (containsAny(changeSet.getValue(), changeIds)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean containsAny(Set<byte[]> set, Collection<byte[]> toSearch) {
    for (byte[] item : toSearch) {
      if (set.contains(item)) {
        return true;
      }
    }
    return false;
  }

  private void makeVisible(Transaction tx) {
    // remove from in-progress set, so that it does not get excluded in the future
    Long previous = inProgress.remove(tx.getWritePointer());
    if (previous == null) {
      // tx was not in progress! perhaps it timed out and is invalid? try to remove it there.
      invalid.rem(tx.getWritePointer());
    }
    // moving read pointer
    moveReadPointerIfNeeded(tx.getWritePointer());
  }

  private void moveReadPointerIfNeeded(long committedWritePointer) {
    if (committedWritePointer > readPointer) {
      readPointer = committedWritePointer;
    }
  }

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

  //--------- helpers to encode or decode the transaction state --------------
  //--------- all these must be called from synchronized context -------------

  private byte[] encodeState() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder encoder = new BinaryEncoder(bos);

    try {
      encoder.writeInt(STATE_PERSIST_VERSION);
      encoder.writeLong(readPointer);
      encoder.writeLong(nextWritePointer);
      encoder.writeLong(waterMark);
      encodeInvalid(encoder);
      encodeInProgress(encoder);
      encodeChangeSets(encoder, committedChangeSets);
      encodeChangeSets(encoder, committingChangeSets);

    } catch (IOException e) {
      LOG.error("Unable to serialize transaction state: ", e);
      throw Throwables.propagate(e);
    }
    return bos.toByteArray();
  }

  private void decodeState(byte[] bytes) {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    Decoder decoder = new BinaryDecoder(bis);

    try {
      int persistedVersion = decoder.readInt();
      if (persistedVersion != STATE_PERSIST_VERSION) {
        throw new RuntimeException("Can't decode state persisted with version " + persistedVersion + ". Current " +
                                     "version is " + STATE_PERSIST_VERSION);
      }
      readPointer = decoder.readLong();
      nextWritePointer = decoder.readLong();
      waterMark = decoder.readLong();
      decodeInvalid(decoder);
      decodeInProgress(decoder);
      decodeChangeSets(decoder, committedChangeSets);
      decodeChangeSets(decoder, committingChangeSets);

    } catch (IOException e) {
      LOG.error("Unable to deserialize transaction state: ", e);
      throw Throwables.propagate(e);
    }
  }

  private void encodeInvalid(Encoder encoder) throws IOException {
    if (!invalid.isEmpty()) {
      encoder.writeInt(invalid.size());
      for (long invalidTx : invalid) {
        encoder.writeLong(invalidTx);
      }
    }
    encoder.writeInt(0); // zero denotes end of list as per AVRO spec
  }

  private void decodeInvalid(Decoder decoder) throws IOException {
    invalid.clear();
    int size = decoder.readInt();
    while (size != 0) { // zero denotes end of list as per AVRO spec
      for (int remaining = size; remaining > 0; --remaining) {
        invalid.add(decoder.readLong());
      }
      size = decoder.readInt();
    }
  }

  private void encodeInProgress(Encoder encoder) throws IOException {
    if (!inProgress.isEmpty()) {
      encoder.writeInt(inProgress.size());
      for (Map.Entry<Long, Long> entry : inProgress.entrySet()) {
        encoder.writeLong(entry.getKey()); // tx id
        encoder.writeLong(entry.getValue()); // time stamp;
      }
    }
    encoder.writeInt(0); // zero denotes end of list as per AVRO spec
  }

  private void decodeInProgress(Decoder decoder) throws IOException {
    inProgress.clear();
    int size = decoder.readInt();
    while (size != 0) { // zero denotes end of list as per AVRO spec
      for (int remaining = size; remaining > 0; --remaining) {
        inProgress.put(decoder.readLong(), decoder.readLong());
      }
      size = decoder.readInt();
    }
  }

  private void encodeChangeSets(Encoder encoder, Map<Long, Set<byte[]>> changes) throws IOException {
    if (!changes.isEmpty()) {
      encoder.writeInt(changes.size());
      for (Map.Entry<Long, Set<byte[]>> entry : changes.entrySet()) {
        encoder.writeLong(entry.getKey());
        encodeChanges(encoder, entry.getValue());
      }
    }
    encoder.writeInt(0); // zero denotes end of list as per AVRO spec
  }

  private void decodeChangeSets(Decoder decoder, Map<Long, Set<byte[]>> changeSets) throws IOException {
    changeSets.clear();
    int size = decoder.readInt();
    while (size != 0) { // zero denotes end of list as per AVRO spec
      for (int remaining = size; remaining > 0; --remaining) {
        changeSets.put(decoder.readLong(), decodeChanges(decoder));
      }
      size = decoder.readInt();
    }
  }

  private void encodeChanges(Encoder encoder, Set<byte[]> changes) throws IOException {
    if (!changes.isEmpty()) {
      encoder.writeInt(changes.size());
      for (byte[] change : changes) {
        encoder.writeBytes(change);
      }
    }
    encoder.writeInt(0); // zero denotes end of list as per AVRO spec
  }

  private Set<byte[]> decodeChanges(Decoder decoder) throws IOException {
    List<byte[]> changes = Lists.newArrayList();
    int size = decoder.readInt();
    while (size != 0) { // zero denotes end of list as per AVRO spec
      for (int remaining = size; remaining > 0; --remaining) {
        changes.add(Bytes.toBytes(decoder.readBytes()));
      }
      size = decoder.readInt();
    }
    return ImmutableSortedSet.copyOf(Bytes.BYTES_COMPARATOR, changes);
  }

  /**
   * Called from the opex service every 10 seconds.
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
}
