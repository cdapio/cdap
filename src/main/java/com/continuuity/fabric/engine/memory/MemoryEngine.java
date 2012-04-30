package com.continuuity.fabric.engine.memory;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import com.continuuity.fabric.engine.Engine;
import com.continuuity.fabric.operations.impl.Modifier;

public class MemoryEngine implements Engine {

  private final Map<byte[],byte[]> kvmap =
      new TreeMap<byte[],byte[]>(new ByteArrayComparator());

  private final Map<byte[],Count> countermap =
      new TreeMap<byte[],Count>(new ByteArrayComparator());

  public byte[] get(byte[] key) {
    return this.kvmap.get(key);
  }

  public void put(byte [] key, byte [] value) {
    this.kvmap.put(key, value);
  }

  public boolean compareAndSwap(byte [] key,
      byte [] expectedValue, byte [] newValue) {
    byte [] existingValue = this.kvmap.get(key);
    if (existingValue == null && expectedValue != null) return false;
    if (existingValue != null && expectedValue == null) return false;
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

  public void increment(byte [] key, long amount) {
    Count count = this.countermap.get(key);
    if (count == null) {
      count = new Count();
      this.countermap.put(key, count);
    }
    count.count += amount;
  }

  public long getCounter(byte [] key) {
    Count count = this.countermap.get(key);
    if (count == null) return 0L;
    return count.count;
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
