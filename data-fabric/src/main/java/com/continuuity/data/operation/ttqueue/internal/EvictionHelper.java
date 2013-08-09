package com.continuuity.data.operation.ttqueue.internal;

import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Eviction helper is used to determine the min queue entry that can be evicted.
 */
public class EvictionHelper {
  // Map of transaction write version to min ack entry of that transaction
  private Map<Long, Long> txnMinAckEntryMap;
  // Max (min committed entry) of committed and cleaned up transactions
  private long maxCommittedCleanedUpEntry = TTQueueConstants.FIRST_ENTRY_ID - 1;

  public EvictionHelper() {
    this.txnMinAckEntryMap = Maps.newHashMap();
  }

  private EvictionHelper(Map<Long, Long> txnMinAckEntryMap, long maxCommittedCleanedUpEntry) {
    this.txnMinAckEntryMap = txnMinAckEntryMap;
    this.maxCommittedCleanedUpEntry = maxCommittedCleanedUpEntry;
  }

  /**
   * Saves the min ack entry of an uncommitted transaction.
   * @param txnPtr Transaction write pointer
   * @param minAckEntry the min ack entry
   */
  public void addMinAckEntry(long txnPtr, long minAckEntry) {
    Long current = txnMinAckEntryMap.get(txnPtr);
    if (current == null || current > minAckEntry) {
      txnMinAckEntryMap.put(txnPtr, minAckEntry);
    }
  }

  /**
   * Returns the min entry that can be evicted for the consumer based on ack list. Min eviction entry will not be
   * greater than minUnackedEntry.
   * Min evict entry is calculated as the first valid of -
   * 1. min(uncommitted entries) - 1
   * 2. max(min committed entries of txns)
   * @param minUnackedEntry the min unacked entry for a consumer
   * @param readPointer transaction read pointer
   * @return min entry that can be evicted for the consumer, INVALID_ENTRY_ID if cannot be determined.
   */
  public long getMinEvictionEntry(long minUnackedEntry, ReadPointer readPointer) {
    // Min evict entry has to be less than the minUnackedEntry
    long minLegalEvictEntry = minUnackedEntry - 1;

    long minUnCommittedAckEntry = Long.MAX_VALUE;
    long maxMinCommittedAckEntry = maxCommittedCleanedUpEntry > minLegalEvictEntry ?
      TTQueueConstants.FIRST_ENTRY_ID - 1 : maxCommittedCleanedUpEntry;

    for (Map.Entry<Long, Long> entry : txnMinAckEntryMap.entrySet()) {
      if (entry.getValue() > minLegalEvictEntry) {
        // This entry is greater than min legal evict entry, ignore it.
        continue;
      }
      // Finalize runs after a commit, so it is safe to consider ack list from current transaction too
      if (readPointer.isVisible(entry.getKey())) {
        // Transaction is committed
        if (maxMinCommittedAckEntry < entry.getValue()) {
          maxMinCommittedAckEntry = entry.getValue();
        }
      } else {
        // Transaction is not yet committed
        if (minUnCommittedAckEntry > entry.getValue()) {
          minUnCommittedAckEntry = entry.getValue();
        }
      }
    }

    if (minUnCommittedAckEntry != Long.MAX_VALUE) {
      return minUnCommittedAckEntry - 1;
    } else if (maxMinCommittedAckEntry >= TTQueueConstants.FIRST_ENTRY_ID) {
      return maxMinCommittedAckEntry;
    } else {
      return TTQueueConstants.INVALID_ENTRY_ID;
    }
  }

  /**
   * Cleans-up the min ack entries from the transactions that are now committed.
   * @param transaction Current transaction
   */
  public void cleanup(Transaction transaction) {
    ReadPointer readPointer = transaction.getReadPointer();

    long maxMinCommittedAckEntry = TTQueueConstants.FIRST_ENTRY_ID - 1;

    Iterator<Map.Entry<Long, Long>> iterator = txnMinAckEntryMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, Long> entry = iterator.next();
      // Cleanup happens within a transaction, so need to exclude current transaction from getting cleaned up
      if (entry.getKey() != transaction.getWriteVersion() && readPointer.isVisible(entry.getKey())) {
        // Save the maxMinCommittedAckEntry
        if (maxMinCommittedAckEntry < entry.getValue()) {
          maxMinCommittedAckEntry = entry.getValue();
        }
        // Transaction is committed, we can remove the ack list safely
        iterator.remove();
      }
    }

    // Compute the maxCommittedCleanedUpEntry
    if (this.maxCommittedCleanedUpEntry < maxMinCommittedAckEntry) {
      this.maxCommittedCleanedUpEntry = maxMinCommittedAckEntry;
    }
  }

  /**
   * Returns the number of transactions present in EvictionHelper.
   * @return number of transactions present in EvictionHelper
   */
  public int size() {
    return txnMinAckEntryMap.size();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("txnMinAckEntryMap", txnMinAckEntryMap)
      .toString();
  }

  public void encode(Encoder encoder) throws IOException {
    // Encode the txnMinAckEntryMap
    if (!txnMinAckEntryMap.isEmpty()) {
      encoder.writeInt(txnMinAckEntryMap.size());
      for (Map.Entry<Long, Long> entry : txnMinAckEntryMap.entrySet()) {
        encoder.writeLong(entry.getKey());
        encoder.writeLong(entry.getValue());
      }
    }
    encoder.writeInt(0); // zero denotes end of map as per AVRO spec

    // Encode the maxCommittedCleanedUpEntry
    encoder.writeLong(maxCommittedCleanedUpEntry);
  }

  public static EvictionHelper decode(Decoder decoder) throws IOException {
    // Decode the txnMinAckEntryMap
    int mapSize = decoder.readInt();
    Map<Long, Long> txnMinAckEntryMap = Maps.newHashMapWithExpectedSize(mapSize);
    while (mapSize > 0) {
      for (int i = 0; i < mapSize; ++i) {
        txnMinAckEntryMap.put(decoder.readLong(), decoder.readLong());
      }
      mapSize = decoder.readInt();
    }

    // Decode the maxCommittedCleanedUpEntry
    long maxCommittedAckEntry = decoder.readLong();
    return new EvictionHelper(txnMinAckEntryMap, maxCommittedAckEntry);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EvictionHelper that = (EvictionHelper) o;

    return maxCommittedCleanedUpEntry == that.maxCommittedCleanedUpEntry &&
      !(txnMinAckEntryMap != null ? !txnMinAckEntryMap.equals(that.txnMinAckEntryMap) : that.txnMinAckEntryMap != null);

  }

  @Override
  public int hashCode() {
    int result = txnMinAckEntryMap != null ? txnMinAckEntryMap.hashCode() : 0;
    result = 31 * result + (int) (maxCommittedCleanedUpEntry ^ (maxCommittedCleanedUpEntry >>> 32));
    return result;
  }
}
