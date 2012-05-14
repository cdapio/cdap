package com.continuuity.fabric.engine.memory;

import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.fabric.engine.NativeSimpleExecutor;
import com.continuuity.fabric.operations.impl.Modifier;

public class MemorySimpleExecutor implements NativeSimpleExecutor {

  private final MemoryEngine engine;

  public MemorySimpleExecutor(MemoryEngine engine) {
    this.engine = engine;
  }

  public byte[] readRandom(byte[] key) {
    return this.engine.get(generateRandomOrderKey(key));
  }

  public void writeRandom(byte [] key, byte [] value) {
    this.engine.put(generateRandomOrderKey(key), value);
  }

  public Map<byte[],byte[]> readOrdered(byte [] key) {
    return this.engine.getAsMap(key);
  }

  /**
   * 
   * @param startKey inclusive
   * @param endKey exclusive
   * @return
   */
  public Map<byte[],byte[]> readOrdered(byte [] startKey, byte [] endKey) {
    return this.engine.get(startKey, endKey);
  }

  /**
   * 
   * @param startKey inclusive
   * @param limit
   * @return
   */
  public Map<byte[],byte[]> readOrdered(byte [] startKey, int limit) {
    return this.engine.get(startKey, limit);
  }

  public void writeOrdered(byte [] key, byte [] value) {
    this.engine.put(key, value);
  }

  public long readCounter(byte[] key) {
    return this.engine.getCounter(generateRandomOrderKey(key));
  }

  public boolean compareAndSwap(byte [] key,
      byte [] expectedValue, byte [] newValue) {
    return this.engine.compareAndSwap(
        generateRandomOrderKey(key), expectedValue, newValue);
  }

  public void readModifyWrite(byte [] key, Modifier<byte[]> modifier) {
    this.engine.readModifyWrite(generateRandomOrderKey(key), modifier);
  }

  public void increment(byte [] key, long amount) {
    this.engine.increment(generateRandomOrderKey(key), amount);
  }

  public long getCounter(byte [] key) {
    return this.engine.getCounter(generateRandomOrderKey(key));
  }

  public void queuePush(byte [] queueName, byte [] queueEntry) {
    this.engine.queuePush(generateRandomOrderKey(queueName), queueEntry);
  }

  public byte [] queuePop(byte [] queueName) {
    return this.engine.queuePop(generateRandomOrderKey(queueName));
  }

  // Private helper methods


  /**
   * Generates a 4-byte hash of the specified key and returns a copy of the
   * specified key with the hash prepended to it.
   * @param key
   * @return 4-byte-hash(key) + key
   */
  private byte[] generateRandomOrderKey(byte [] key) {
    byte [] hash = Bytes.toBytes(Bytes.hashCode(key));
    return Bytes.add(hash, key);
  }
}
