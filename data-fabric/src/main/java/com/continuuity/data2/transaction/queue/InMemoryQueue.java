package com.continuuity.data2.transaction.queue;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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
      if (updateStartKey) {
        consumerState.startKey = key;
      }
      if (tx.getReadPointer() < key.txId) {
        // the entry is newer than the current transaction. so are all subsequent entries. bail out.
        break;
      } else if (Arrays.binarySearch(tx.getExcludedList(), key.txId) >= 0) {
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
        if (item.claim(config.getGroupId())) {
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
      if (hash % config.getGroupSize() == config.getInstanceId()) {
        keys.add(key);
        datas.add(item.entry.getData());
        updateStartKey = false;
      }
    }
    return keys.isEmpty() ? null : new ImmutablePair<List<Key>, List<byte[]>>(keys, datas);
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
      item.setConsumerState(config.getGroupId(), ConsumerEntryState.PROCESSED);
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
      ConsumerEntryState state = item.removeConsumerState(config.getGroupId());
    }
  }

  public void evict(List<Key> dequeuedKeys, ConsumerConfig config) {
    if (config.getNumGroups() < 1) {
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
      if (item.incrementProcessed() >= config.getNumGroups()) {
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
    ConcurrentMap<Long, ConsumerEntryState> consumerStates = Maps.newConcurrentMap();
    AtomicInteger processedCount = new AtomicInteger();

    Item(QueueEntry entry) {
      this.entry = entry;
    }

    ConsumerEntryState getConsumerState(long consumerGroupId) {
      return consumerStates.get(consumerGroupId);
    }

    void setConsumerState(long consumerGroupId, ConsumerEntryState newState) {
      consumerStates.put(consumerGroupId, newState);
    }

    ConsumerEntryState removeConsumerState(long consumerGroupId) {
      return consumerStates.remove(consumerGroupId);
    }

    boolean claim(long consumerGroupId) {
      return consumerStates.putIfAbsent(consumerGroupId, ConsumerEntryState.CLAIMED) == null;
    }

    int incrementProcessed() {
      return processedCount.incrementAndGet();
    }
  }

  /**
   * The state of a single consumer, gets modified.
   */
  public static class ConsumerState {
    Key startKey = null;
  }

}
