package com.continuuity.fabric.engine.memory;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.fabric.engine.Engine;
import com.continuuity.fabric.operations.impl.Modifier;

public class MemoryEngine implements Engine {

  private final TreeMap<byte[],byte[]> kvmap =
      new TreeMap<byte[],byte[]>(new ByteArrayComparator());

  private final TreeMap<byte[],Count> countermap =
      new TreeMap<byte[],Count>(new ByteArrayComparator());

  private final Map<byte[],Queue<byte[]>> queuemap =
      new TreeMap<byte[],Queue<byte[]>>(new ByteArrayComparator());

  public byte[] get(byte[] key) {
    return this.kvmap.get(key);
  }

  public void put(byte [] key, byte [] value) {
    this.kvmap.put(key, value);
  }

  public Map<byte[],byte[]> get(byte [] startKey, byte [] endKey) {
    return this.kvmap.subMap(startKey, endKey);
  }

  public Map<byte[],byte[]> getAsMap(byte[] key) {
    byte [] value = get(key);
    if (value == null) return null;
    Map<byte[],byte[]> map =
        new TreeMap<byte[],byte[]>(new ByteArrayComparator());
    map.put(key, value);
    return map;
  }

  public Map<byte[],byte[]> get(byte [] startKey, int limit) {
    Map<byte[],byte[]> map = this.kvmap.tailMap(startKey);
    if (map == null || map.size() <= limit) return map;
    Map<byte[],byte[]> limitMap =
        new TreeMap<byte[],byte[]>(new ByteArrayComparator());
    int n = 0;
    for (Map.Entry<byte[],byte[]> entry : map.entrySet()) {
      limitMap.put(entry.getKey(), entry.getValue());
      if (++n == limit) break;
    }
    return limitMap;
  }

  public boolean compareAndSwap(byte [] key,
      byte [] expectedValue, byte [] newValue) {
    byte [] existingValue = this.kvmap.get(key);
    if (existingValue == null && expectedValue != null) return false;
    if (existingValue != null && expectedValue == null) return false;
    if (!Bytes.equals(existingValue, expectedValue)) return false;
    this.kvmap.put(key, newValue);
    return true;
  }

  public void readModifyWrite(byte [] key, Modifier<byte[]> modifier) {
    while (true) {
      byte [] existingValue = get(key);
      byte [] newValue = modifier.modify(existingValue);
      if (compareAndSwap(key, existingValue, newValue)) break;
    }
  }

  public long increment(byte [] key, long amount) {
    Count count = this.countermap.get(key);
    if (count == null) {
      count = new Count();
      this.countermap.put(key, count);
    }
    count.count += amount;
    return count.count;
  }

  public long getCounter(byte [] key) {
    Count count = this.countermap.get(key);
    if (count == null) return 0L;
    return count.count;
  }

  public void queuePush(byte [] queueName, byte [] queueEntry) {
    Queue<byte[]> queue = this.queuemap.get(queueName);
    if (queue == null) {
      queue = new LinkedList<byte[]>();
      this.queuemap.put(queueName, queue);
    }
    queue.add(queueEntry);
  }

  public byte [] queuePop(byte [] queueName) {
    Queue<byte[]> queue = this.queuemap.get(queueName);
    if (queue == null) {
      return null;
    }
    return queue.poll();
    
  }
  public static class ByteArrayComparator implements Comparator<byte[]> {
    @Override
    public int compare(byte[] o1, byte[] o2) {
      // hehe :)
      return new String(o1).compareTo(new String(o2));
    }
  }

  public static class Count {
    public long count = 0;
  }
}
