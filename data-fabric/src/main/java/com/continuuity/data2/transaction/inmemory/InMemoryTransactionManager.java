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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 */
// todo: synchronize all
// todo: optimize heavily
public class InMemoryTransactionManager {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTransactionManager.class);

  // Minimum size of the excludedList for compaction. This is to avoid compacting on every commit.
  private static final int MIN_EXCLUDE_LIST_SIZE = 1000;

  // How many write versions to claim at a time, by default one million
  public static final String CFG_TX_CLAIM_SIZE = "data.tx.claim.size";
  public static final int DEFAULT_TX_CLAIM_SIZE = 1000 * 1000;

  private static final String ALL_STATE_TAG = "all";
  private static final String WATERMARK_TAG = "mark";

  private LongArrayList excludedList;

  // todo: use moving array instead (use Long2ObjectMap<byte[]> in fastutil)
  // commit time nextWritePointer -> changes made by this tx
  private NavigableMap<Long, Set<byte[]>> committedChangeSets;
  // not committed yet
  private Map<Long, Set<byte[]>> committingChangeSets;

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

  public InMemoryTransactionManager() {
    persistor = null;
    reset();
    initialized = true;
  }

  @Inject
  public InMemoryTransactionManager(CConfiguration conf, @Nullable StatePersistor persistor) {
    this.persistor = persistor;
    claimSize = conf.getInt(CFG_TX_CLAIM_SIZE, DEFAULT_TX_CLAIM_SIZE);
    reset();
  }

  private void reset() {
    excludedList = new LongArrayList();
    committedChangeSets = Maps.newTreeMap();
    committingChangeSets = Maps.newHashMap();
    readPointer = 0;
    nextWritePointer = 1;
    waterMark = 0; // this will trigger a claim at the first start transaction
  }

  // TODO this class should implement Service and this should be start().
  // TODO However, start() is alredy used to start a transaction, so this would be major refactoring now.
  public void init() {
    // establish defaults in case there is no persistence
    reset();

    // try to recover persisted state
    if (persistor != null) {
      try {
        // start up the persistor first
        persistor.start();
        // attempt to restore the full state
        byte[] state;
        state = persistor.readBack(ALL_STATE_TAG);
        if (state != null) {
          decodeState(state);
          LOG.debug("Restored transaction state successfully.");
          persistor.delete(ALL_STATE_TAG);
          return;
        }
        // full state is not there, attempt to restore the watermark
        state = persistor.readBack(WATERMARK_TAG);
        if (state != null) {
          waterMark = Bytes.toLong(state);
          // must have crashed last time... need to claim the next batch of write versions
          waterMark += claimSize;
          readPointer = waterMark - 1;
          nextWritePointer = waterMark; //
          LOG.debug("Recovered transaction watermark successfully, but transaction state may have been lost.");
        }
      } catch (IOException e) {
        LOG.error("Unable to read back transaction state:", e);
        throw Throwables.propagate(e);
      }
    }
    initialized = true;
  }

  public synchronized void close() {
    if (initialized && persistor != null) {
      byte[] state = encodeState();
      try {
        persistor.persist(ALL_STATE_TAG, state);
        LOG.debug("Successfully persisted transaction state.");
      } catch (IOException e) {
        LOG.error("Unable to persist transaction state:", e);
        throw Throwables.propagate(e);
      }
    }
  }

  // not synchronized because itis only called from start() which is synchronized
  private void saveWaterMarkIfNeeded() {
    if (persistor != null) {
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
  }

  public synchronized Transaction start() {
    saveWaterMarkIfNeeded();
    Transaction tx = new Transaction(readPointer, nextWritePointer, getExcludedListAsArray(excludedList));
    excludedList.add(nextWritePointer);
    // it is important to keep it sorted, as client logic may depend on that
    // Using Collections.sort is somewhat inefficient as it convert the elements into Object[] and put it back.
    // todo: optimize the data structure.
    Collections.sort(excludedList);
    nextWritePointer++;
    return tx;
  }

  public synchronized boolean canCommit(Transaction tx, Collection<byte[]> changeIds) {
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

    if (changeSet != null) {
      // double-checking if there are conflicts: someone may have committed since canCommit check
      if (hasConflicts(tx, changeSet)) {
        return false;
      }

      // Record the committed change set with the nextWritePointer as the commit time.
      if (committedChangeSets.containsKey(nextWritePointer)) {
        committedChangeSets.get(nextWritePointer).addAll(changeSet);
      } else {
        TreeSet<byte[]> committedChangeSet = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
        committedChangeSet.addAll(changeSet);
        committedChangeSets.put(nextWritePointer, committedChangeSet);
      }
    }
    makeVisible(tx);

    // Cleanup commitedChangeSets.
    // All committed change sets that are smaller than the earliest started transaction could be removed.
    committedChangeSets.headMap(excludedList.isEmpty() ? Long.MAX_VALUE : excludedList.get(0)).clear();
    return true;
  }

  public synchronized boolean abort(Transaction tx) {
    committingChangeSets.remove(tx.getWritePointer());
    // makes tx visible (assumes that all operations were rolled back)
    makeVisible(tx);
    return true;
  }

  // hack for exposing important metric
  public int getExcludedListSize() {
    return excludedList.size();
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

  // todo: move to Tx?
  private static boolean visible(Transaction tx, long pointer) {
    if (pointer > tx.getReadPointer()) {
      return false;
    }

    // todo: optimize heavily
    // we rely on array of excludes to be sorted
    return Arrays.binarySearch(tx.getExcludedList(), pointer) < 0;
  }

  private void makeVisible(Transaction tx) {
    int idx = Arrays.binarySearch(excludedList.elements(), 0, excludedList.size(), tx.getWritePointer());
    if (idx >= 0) {
      excludedList.removeLong(idx);
      // trim is needed as remove of LongArrayList would not shrink the backing array.
      // trim will do nothing if the size of excludedList is smaller than the MIN_EXCLUDE_LIST_SIZE.
      excludedList.trim(MIN_EXCLUDE_LIST_SIZE);
    }
    // moving read pointer
    moveReadPointerIfNeeded(tx.getWritePointer());
  }

  private void moveReadPointerIfNeeded(long committedWritePointer) {
    if (committedWritePointer > readPointer) {
      readPointer = committedWritePointer;
    }
  }

  private static long[] getExcludedListAsArray(LongArrayList excludedList) {
    // todo: optimize (cache, etc. etc.)
    long[] elements = excludedList.elements();
    return Arrays.copyOf(elements, excludedList.size());
  }

  //--------- helpers to encode or decode the transaction state --------------
  //--------- all these must be called from synchronized context -------------

  private byte[] encodeState() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder encoder = new BinaryEncoder(bos);

    try {
      // encode read pointer
      encoder.writeLong(readPointer);
      // encode next write pointer
      encoder.writeLong(nextWritePointer);
      // encode watermark
      encoder.writeLong(waterMark);
      // encode excluded list
      encodeExcluded(encoder);
      // encode committed change sets
      encodeChangeSets(encoder, committedChangeSets);
      // encode committing change set
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
      // decode read pointer
      readPointer = decoder.readLong();
      // decode next write pointer
      nextWritePointer = decoder.readLong();
      // decode watermark
      waterMark = decoder.readLong();
      // decode excluded list
      decodeExcluded(decoder);
      // decode committed change sets
      decodeChangeSets(decoder, committedChangeSets);
      // decode committing change set
      decodeChangeSets(decoder, committingChangeSets);

    } catch (IOException e) {
      LOG.error("Unable to deserialize transaction state: ", e);
      throw Throwables.propagate(e);
    }
  }

  private void encodeExcluded(Encoder encoder) throws IOException {
    if (!excludedList.isEmpty()) {
      encoder.writeInt(excludedList.size());
      for (long exclude : excludedList) {
        encoder.writeLong(exclude);
      }
    }
    encoder.writeInt(0); // zero denotes end of list as per AVRO spec
  }

  private void decodeExcluded(Decoder decoder) throws IOException {
    excludedList.clear();
    int size = decoder.readInt();
    while (size != 0) { // zero denotes end of list as per AVRO spec
      for (int remaining = size; remaining > 0; --remaining) {
        excludedList.add(decoder.readLong());
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
}
