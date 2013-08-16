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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of an in-memory queue.
 */
public class InMemoryQueue {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryQueue.class);

  final ConcurrentNavigableMap<Key, Item> entries = new ConcurrentSkipListMap<Key, Item>();

  public void enqueue(long txId, int seqId, QueueEntry entry) {
    entries.put(new Key(txId, seqId), new Item(entry));
  }

  public void undoEnqueue(long txId, int seqId) {
    entries.remove(new Key(txId, seqId));
  }

  public ImmutablePair<List<Key>, List<byte[]>> dequeue(Transaction tx, ConsumerConfig config, int maxBatchSize) {

    List<Key> keys = Lists.newArrayListWithCapacity(maxBatchSize);
    List<byte[]> datas = Lists.newArrayListWithCapacity(maxBatchSize);

    // navigableKeySet is immune to concurrent modification
    for (Key key : entries.navigableKeySet()) {
      if (tx.getReadPointer() < key.txId) {
        // the entry is newer than the current transaction. so are all subsequent entries. bail out.
        break;
      } else if (Arrays.binarySearch(tx.getExcludedList(), key.txId) >= 0) {
        // the entry is in the exclude list of current transaction. There is a chance that visible entries follow.
        continue;
      }
      Item item = entries.get(key);
      if (item == null) {
        // entry was deleted (evicted or undone) after we started iterating
        continue;
      }
      if (config.getDequeueStrategy().equals(DequeueStrategy.FIFO)) {
        // for FIFO, attempt to claim the entry and return it
        if (item.claim(config.getGroupId())) {
          keys.add(key);
          datas.add(item.entry.getData());
        } else {
          continue; // someone else claimed it, or it was already processed, move on
        }
      }
      // check whether this is processed already
      ConsumerEntryState state = item.getConsumerState(config.getGroupId());
      if (ConsumerEntryState.PROCESSED.equals(state)) {
        // already processed but not yet evicted. move on
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
      }
    }
    return keys.isEmpty() ? null : new ImmutablePair<List<Key>, List<byte[]>>(keys, datas);
  }

  public void ack(List<Key> dequeuedKeys, ConsumerConfig config) {
    for (Key key : dequeuedKeys) {
      Item item = entries.get(key);
      if (item == null) {
        LOG.warn("Attempting to ack non-existing entry " + key);
        continue;
      }
      item.setConsumerState(config.getGroupId(), ConsumerEntryState.PROCESSED);
      item.incrementProcessed();
    }
  }

  public void undoDequeue(List<Key> dequeuedKeys, ConsumerConfig config) {
    for (Key key : dequeuedKeys) {
      Item item = entries.get(key);
      if (item == null) {
        LOG.warn("Attempting to undo dequeue for non-existing entry " + key);
        continue;
      }
      ConsumerEntryState state = item.removeConsumerState(config.getGroupId());
      if (ConsumerEntryState.PROCESSED.equals(state)) {
        item.decrementProcessed();
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

    void decrementProcessed() {
      processedCount.decrementAndGet();
    }
  }

}
