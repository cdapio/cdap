package com.continuuity.data2.transaction.queue.inmemory;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.queue.QueueEntry;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.queue.ConsumerEntryState;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of an in-memory queue.
 */
public class InMemoryQueue {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryQueue.class);

  private final ConcurrentNavigableMap<Key, Item> entries = new ConcurrentSkipListMap<Key, Item>();

  public void clear() {
    entries.clear();
  }

  public int getSize() {
    return entries.size();
  }

  public void enqueue(long txId, int seqId, QueueEntry entry) {
    entries.put(new Key(txId, seqId), new Item(entry));
  }

  public void undoEnqueue(long txId, int seqId) {
    entries.remove(new Key(txId, seqId));
  }

  public ImmutablePair<List<Key>, List<byte[]>> dequeue(Transaction tx, ConsumerConfig config,
                                                        ConsumerState consumerState, int maxBatchSize) {

    List<Key> keys = Lists.newArrayListWithCapacity(maxBatchSize);
    List<byte[]> datas = Lists.newArrayListWithCapacity(maxBatchSize);
    NavigableSet<Key> keysToScan = consumerState.startKey == null ? entries.navigableKeySet() :
      entries.tailMap(consumerState.startKey).navigableKeySet();
    boolean updateStartKey = true;

    // navigableKeySet is immune to concurrent modification
    for (Key key : keysToScan) {
      if (keys.size() >= maxBatchSize) {
        break;
      }
      if (updateStartKey && key.txId < tx.getFirstShortInProgress()) {
        // See QueueEntryRow#canCommit for reason.
        consumerState.startKey = key;
      }
      if (tx.getReadPointer() < key.txId) {
        // the entry is newer than the current transaction. so are all subsequent entries. bail out.
        break;
      } else if (tx.isInProgress(key.txId)) {
        // the entry is in the exclude list of current transaction. There is a chance that visible entries follow.
        updateStartKey = false; // next time we have to revisit this entry
        continue;
      }
      Item item = entries.get(key);
      if (item == null) {
        // entry was deleted (evicted or undone) after we started iterating
        continue;
      }
      // check whether this is processed already
      ConsumerEntryState state = item.getConsumerState(config.getGroupId());
      if (ConsumerEntryState.PROCESSED.equals(state)) {
        // already processed but not yet evicted. move on
        continue;
      }
      if (config.getDequeueStrategy().equals(DequeueStrategy.FIFO)) {
        // for FIFO, attempt to claim the entry and return it
        if (item.claim(config)) {
          keys.add(key);
          datas.add(item.entry.getData());
        }
        // else: someone else claimed it, or it was already processed, move on, but we may have to revisit this.
        updateStartKey = false;
        continue;
      }
      // for hash/round robin, if group size is 1, just take it
      if (config.getGroupSize() == 1) {
        keys.add(key);
        datas.add(item.entry.getData());
        updateStartKey = false;
        continue;
      }
      // hash by entry hash key or entry id
      int hash;
      if (config.getDequeueStrategy().equals(DequeueStrategy.ROUND_ROBIN)) {
        hash = key.hashCode();
      } else {
        Integer hashFoundInEntry = item.entry.getHashKey(config.getHashKey());
        hash = hashFoundInEntry == null ? 0 : hashFoundInEntry;
      }
      // modulo of a negative is negative, make sure we're positive or 0.
      if (Math.abs(hash) % config.getGroupSize() == config.getInstanceId()) {
        keys.add(key);
        datas.add(item.entry.getData());
        updateStartKey = false;
      }
    }
    return keys.isEmpty() ? null : ImmutablePair.of(keys, datas);
  }

  public void ack(List<Key> dequeuedKeys, ConsumerConfig config) {
    if (dequeuedKeys == null) {
      return;
    }
    for (Key key : dequeuedKeys) {
      Item item = entries.get(key);
      if (item == null) {
        LOG.warn("Attempting to ack non-existing entry " + key);
        continue;
      }
      item.setConsumerState(config, ConsumerEntryState.PROCESSED);
    }
  }

  public void undoDequeue(List<Key> dequeuedKeys, ConsumerConfig config) {
    if (dequeuedKeys == null) {
      return;
    }
    for (Key key : dequeuedKeys) {
      Item item = entries.get(key);
      if (item == null) {
        LOG.warn("Attempting to undo dequeue for non-existing entry " + key);
        continue;
      }
      item.revokeConsumerState(config, config.getDequeueStrategy() == DequeueStrategy.FIFO);
    }
  }

  public void evict(List<Key> dequeuedKeys, int numGroups) {
    if (numGroups < 1) {
      return; // this means no eviction because number of groups is not known
    }
    if (dequeuedKeys == null) {
      return;
    }
    for (Key key : dequeuedKeys) {
      Item item = entries.get(key);
      if (item == null) {
        LOG.warn("Attempting to evict non-existing entry " + key);
        continue;
      }
      if (item.incrementProcessed() >= numGroups) {
        // all consumer groups have processed _and_ reached the post-commit hook: safe to evict
        entries.remove(key);
      }
    }
  }

  /**
   * Used as the key of each queue item, composed of a transaction id and a sequence number within the transaction.
   */
  public static final class Key implements Comparable<Key> {
    final long txId;
    final int seqNo;

    Key(long tx, int seq) {
      txId = tx;
      seqNo = seq;
    }

    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj == null || obj.getClass() != Key.class) {
        return false;
      }
      Key other = (Key) obj;
      return txId == other.txId && seqNo == other.seqNo;
    }

    @Override
    public int hashCode() {
      // return ((int) (txId >> 32)) ^ ((int) txId) ^ seqNo;
      return Objects.hashCode(txId, seqNo);
    }

    @Override
    public int compareTo(Key o) {
      if (txId == o.txId) {
        return seqNo == o.seqNo ? 0 : seqNo < o.seqNo ? -1 : 1;
      } else {
        return txId < o.txId ? -1 : 1;
      }
    }

    @Override
    public String toString() {
      return txId + ":" + seqNo;
    }
  }

  // represents an entry of the queue plus meta data
  private static final class Item {
    final QueueEntry entry;
//    ConcurrentMap<Long, ConsumerEntryState> consumerStates = Maps.newConcurrentMap();
    ConcurrentMap<Long, ItemEntryState> consumerStates = Maps.newConcurrentMap();
    AtomicInteger processedCount = new AtomicInteger();

    Item(QueueEntry entry) {
      this.entry = entry;
    }

    ConsumerEntryState getConsumerState(long consumerGroupId) {
      ItemEntryState entryState = consumerStates.get(consumerGroupId);
      return entryState == null ? null : entryState.getState();
    }

    void setConsumerState(ConsumerConfig config, ConsumerEntryState newState) {
      consumerStates.put(config.getGroupId(), new ItemEntryState(config.getInstanceId(), newState));
    }

    void revokeConsumerState(ConsumerConfig config, boolean revokeToClaim) {
      if (revokeToClaim) {
        consumerStates.put(config.getGroupId(), new ItemEntryState(config.getInstanceId(), ConsumerEntryState.CLAIMED));
      } else {
        consumerStates.remove(config.getGroupId());
      }
    }

    boolean claim(ConsumerConfig config) {
      ItemEntryState state = consumerStates.get(config.getGroupId());
      if (state == null) {
        state = consumerStates.putIfAbsent(config.getGroupId(),
                                           new ItemEntryState(config.getInstanceId(), ConsumerEntryState.CLAIMED));
        if (state == null) {
          return true;
        }
      }
      // If the old claimed consumer is gone or if it has been claimed by the same consumer before,
      // then it can be claimed.
      return state.getInstanceId() >= config.getGroupSize()
        || (state.getInstanceId() == config.getInstanceId() && state.getState() == ConsumerEntryState.CLAIMED);
    }

    int incrementProcessed() {
      return processedCount.incrementAndGet();
    }
  }

  /**
   * Represents the state of an item entry.
   */
  private static final class ItemEntryState {
    final int instanceId;
    ConsumerEntryState state;

    ItemEntryState(int instanceId, ConsumerEntryState state) {
      this.instanceId = instanceId;
      this.state = state;
    }

    int getInstanceId() {
      return instanceId;
    }

    ConsumerEntryState getState() {
      return state;
    }

    void setState(ConsumerEntryState state) {
      this.state = state;
    }
  }

  /**
   * The state of a single consumer, gets modified.
   */
  public static class ConsumerState {
    Key startKey = null;
  }

}
