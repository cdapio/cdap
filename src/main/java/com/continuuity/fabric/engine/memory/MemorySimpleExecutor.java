package com.continuuity.fabric.engine.memory;

import com.continuuity.fabric.engine.NativeSimpleExecutor;
import com.continuuity.fabric.operations.impl.Modifier;

public class MemorySimpleExecutor implements NativeSimpleExecutor {

  private final MemoryEngine engine;

  public MemorySimpleExecutor(MemoryEngine engine) {
    this.engine = engine;
  }

  public byte[] read(byte[] key) {
    return this.engine.get(key);
  }

  public void write(byte [] key, byte [] value) {
    this.engine.put(key, value);
  }

  public boolean compareAndSwap(byte [] key,
      byte [] expectedValue, byte [] newValue) {
    return this.engine.compareAndSwap(key, expectedValue, newValue);
  }

  public void readModifyWrite(byte [] key, Modifier<byte[]> modifier) {
    this.engine.readModifyWrite(key, modifier);
  }

  public void increment(byte [] key, long amount) {
    this.engine.increment(key, amount);
  }

  public long getCounter(byte [] key) {
    return this.engine.getCounter(key);
  }
}
