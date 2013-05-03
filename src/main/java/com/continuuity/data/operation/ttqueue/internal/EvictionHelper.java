package com.continuuity.data.operation.ttqueue.internal;

import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.ttqueue.TTQueueNewOnVCTable;
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

  public EvictionHelper() {
    this.txnMinAckEntryMap = Maps.newHashMap();
  }

  public EvictionHelper(Map<Long, Long> txnMinAckEntryMap) {
    this.txnMinAckEntryMap = txnMinAckEntryMap;
  }

  /**
   * Saves the min ack entry of an uncommitted transaction
   * @param txnPtr Transaction write pointer
   * @param minAckEntry the min ack entry
   */
  public void addMinAckEntry(long txnPtr, long minAckEntry) {
    Long current = txnMinAckEntryMap.get(txnPtr);
    if(current == null || current > minAckEntry) {
      txnMinAckEntryMap.put(txnPtr, minAckEntry);
    }
  }

  /**
   * Returns the min entry that can be evicted for the consumer based on ack list, dequeue entry set and
   * consumer read pointer.
   * Min evict entry is calculated as the first valid of -
   * 1. min(uncommitted entries) - 1
   * 2. max(committed entries)
   * 3. min(dequeue entry set) - 1
   * 4. consumer read pointer - 1
   * @param consumerReadPointer consumer read pointer
   * @param dequeuedEntrySet dequeue entry set
   * @param readPointer transacition read pointer
   * @return min entry that can be evicted for the consumer
   */
  public long getMinEvictionEntry(long consumerReadPointer, TTQueueNewOnVCTable.DequeuedEntrySet dequeuedEntrySet,
                                  ReadPointer readPointer) {
    long minUnCommittedAckEntry = Long.MAX_VALUE;
    long maxCommittedAckEntry = -1;  // TODO: use constant

    for(Map.Entry<Long, Long> entry : txnMinAckEntryMap.entrySet()) {
      // Finalize runs after a commit, so it is safe to consider ack list from current transaction too
      if(readPointer.isVisible(entry.getKey())) {
        // Transaction is committed
        if(maxCommittedAckEntry < entry.getValue()) {
          maxCommittedAckEntry = entry.getValue();
        }
      } else {
        // Transaction is not yet committed
        if(minUnCommittedAckEntry > entry.getValue()) {
          minUnCommittedAckEntry = entry.getValue();
        }
      }
    }
    if(minUnCommittedAckEntry != Long.MAX_VALUE) {
      return minUnCommittedAckEntry - 1;
    } else if(maxCommittedAckEntry != -1) {
      return maxCommittedAckEntry;
    } else if(!dequeuedEntrySet.isEmpty()) {
      return dequeuedEntrySet.min().getEntryId() - 1;
    } else {
      return consumerReadPointer - 1;
    }
  }

  /**
   * Cleans-up the min ack entries from the transactions that are now committed
   * @param transaction Current transaction
   */
  public void cleanup(Transaction transaction) {
    ReadPointer readPointer = transaction.getReadPointer();
    Iterator<Map.Entry<Long, Long>> iterator = txnMinAckEntryMap.entrySet().iterator();
    while(iterator.hasNext()) {
      Map.Entry<Long, Long> entry = iterator.next();
      // Cleanup happens within a transaction, so need to exclude current transaction from getting cleaned up
      if(entry.getKey() != transaction.getWriteVersion() &&  readPointer.isVisible(entry.getKey())) {
        // Transaction is committed, we can remove the ack list safely
        iterator.remove();
      }
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("txnMinAckEntryMap", txnMinAckEntryMap)
      .toString();
  }

  public void encode(Encoder encoder) throws IOException {
    if(!txnMinAckEntryMap.isEmpty()) {
      encoder.writeInt(txnMinAckEntryMap.size());
      for(Map.Entry<Long, Long> entry : txnMinAckEntryMap.entrySet()) {
        encoder.writeLong(entry.getKey());
        encoder.writeLong(entry.getValue());
      }
    }
    encoder.writeInt(0); // zero denotes end of map as per AVRO spec
  }

  public static EvictionHelper decode(Decoder decoder) throws IOException {
    int mapSize = decoder.readInt();
    Map<Long, Long> txnMinAckEntryMap = Maps.newHashMapWithExpectedSize(mapSize);
    while(mapSize > 0) {
      for(int i= 0; i < mapSize; ++i) {
        txnMinAckEntryMap.put(decoder.readLong(), decoder.readLong());
      }
      mapSize = decoder.readInt();
    }
    return new EvictionHelper(txnMinAckEntryMap);
  }
}
