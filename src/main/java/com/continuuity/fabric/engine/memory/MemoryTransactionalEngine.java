package com.continuuity.fabric.engine.memory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.fabric.engine.Engine;
import com.continuuity.fabric.operations.impl.Modifier;
import com.continuuity.fabric.operations.queues.QueueConsumer;
import com.continuuity.fabric.operations.queues.QueueEntry;
import com.continuuity.fabric.operations.queues.QueuePartitioner;

public class MemoryTransactionalEngine implements Engine {

  private final AtomicLong txid = new AtomicLong(0);

  private final AtomicLong lowTx = new AtomicLong(0);

  private final Set<Long> includedExceptions = new HashSet<Long>();
  
  private final Set<Long> excludedExceptions = new HashSet<Long>();
  
  private final TreeMap<TransactionalByteArray,byte[]> kvmap =
      new TreeMap<TransactionalByteArray,byte[]>();

  private final TreeMap<TransactionalByteArray,Count> countermap =
      new TreeMap<TransactionalByteArray,Count>();

  private final ConcurrentHashMap<TransactionalByteArray,MemoryQueue> queuemap =
      new ConcurrentHashMap<TransactionalByteArray,MemoryQueue>();

  public byte[] get(byte[] key) {
    TransactionalByteArray txKey = new TransactionalByteArray(key);
    return this.kvmap.get(key);
  }
//
//  public void put(byte [] key, byte [] value) {
//    this.kvmap.put(key, value);
//  }
//
//  public Map<byte[],byte[]> get(byte [] startKey, byte [] endKey) {
//    return this.kvmap.subMap(startKey, endKey);
//  }
//
//  public Map<byte[],byte[]> getAsMap(byte[] key) {
//    byte [] value = get(key);
//    if (value == null) return null;
//    Map<byte[],byte[]> map =
//        new TreeMap<byte[],byte[]>(new Bytes.ByteArrayComparator());
//    map.put(key, value);
//    return map;
//  }
//
//  public Map<byte[],byte[]> get(byte [] startKey, int limit) {
//    Map<byte[],byte[]> map = this.kvmap.tailMap(startKey);
//    if (map == null || map.size() <= limit) return map;
//    Map<byte[],byte[]> limitMap =
//        new TreeMap<byte[],byte[]>(new Bytes.ByteArrayComparator());
//    int n = 0;
//    for (Map.Entry<byte[],byte[]> entry : map.entrySet()) {
//      limitMap.put(entry.getKey(), entry.getValue());
//      if (++n == limit) break;
//    }
//    return limitMap;
//  }
//
//  public boolean compareAndSwap(byte [] key,
//      byte [] expectedValue, byte [] newValue) {
//    byte [] existingValue = this.kvmap.get(key);
//    if (existingValue == null && expectedValue != null) return false;
//    if (existingValue != null && expectedValue == null) return false;
//    if (!Bytes.equals(existingValue, expectedValue)) return false;
//    this.kvmap.put(key, newValue);
//    return true;
//  }
//
//  public void readModifyWrite(byte [] key, Modifier<byte[]> modifier) {
//    while (true) {
//      byte [] existingValue = get(key);
//      byte [] newValue = modifier.modify(existingValue);
//      if (compareAndSwap(key, existingValue, newValue)) break;
//    }
//  }
//
//  public long increment(byte [] key, long amount) {
//    Count count = this.countermap.get(key);
//    if (count == null) {
//      count = new Count();
//      this.countermap.put(key, count);
//    }
//    count.count += amount;
//    return count.count;
//  }
//
//  public long getCounter(byte [] key) {
//    Count count = this.countermap.get(key);
//    if (count == null) return 0L;
//    return count.count;
//  }
//
//  public boolean queuePush(byte [] queueName, byte [] queueEntry) {
//    MemoryQueue queue = initQueue(queueName);
//    return queue.push(queueEntry);
//  }
//
//  public boolean queueAck(byte[] queueName, QueueEntry queueEntry) {
//    MemoryQueue queue = this.queuemap.get(new TransactionalByteArray(queueName, 1));
//    if (queue == null) return false;
//    return queue.ack(queueEntry);
//  }
//
//  public QueueEntry queuePop(byte [] queueName, QueueConsumer consumer,
//      QueuePartitioner partitioner) throws InterruptedException {
//    MemoryQueue queue = initQueue(queueName);
//    return queue.pop(consumer, partitioner);
//  }
//
//  private MemoryQueue initQueue(byte[] queueName) {
//    MemoryQueue queue = this.queuemap.get(new TransactionalByteArray(queueName, 1));
//    if (queue != null) return queue;
//    queue = new MemoryQueue();
//    MemoryQueue raceQueue =
//        this.queuemap.putIfAbsent(new TransactionalByteArray(queueName, 1), queue);
//    if (raceQueue != null) queue = raceQueue;
//    return queue;
//  }

  public static class Count {
    public long count = 0;
  }

  public static class TransactionalByteArray
  implements Comparable<TransactionalByteArray> {
    private final byte [] bytes;
    private final int hash;
    private final long txid;
    public TransactionalByteArray(byte [] bytes, long txid) {
      this.bytes = bytes;
      this.hash = Bytes.hashCode(bytes);
      this.txid = txid;
    }
    /**
     * Constructs a transactional byte array using Long.MAX_VALUE as the txid.
     * @param bytes
     */
    public TransactionalByteArray(byte[] bytes) {
      this(bytes, Long.MAX_VALUE);
    }
    @Override
    public int hashCode() {
      return this.hash;
    }
    @Override
    public boolean equals(Object o) {
      TransactionalByteArray ob = (TransactionalByteArray)o;
      if (ob.hashCode() != this.hash) return false;
      return Bytes.equals(this.bytes, ob.getBytes());
    }
    public byte[] getBytes() {
      return this.bytes;
    }
    public long getTxid() {
      return this.txid;
    }
    @Override
    public int compareTo(TransactionalByteArray o) {
      int ret = Bytes.compareTo(getBytes(), o.getBytes());
      if (ret != 0) return ret;
      // order later transactions first
      if (o.getTxid() < getTxid()) return -1;
      if (o.getTxid() > getTxid()) return 1;
      return 0;
    }

  }
}
