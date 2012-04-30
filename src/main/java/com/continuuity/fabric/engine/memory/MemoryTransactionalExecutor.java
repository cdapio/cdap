package com.continuuity.fabric.engine.memory;

import com.continuuity.fabric.engine.NativeTransactionalExecutor;

public class MemoryTransactionalExecutor implements NativeTransactionalExecutor {

  @SuppressWarnings("unused")
  private final MemoryEngine engine;

  public MemoryTransactionalExecutor(MemoryEngine engine) {
    this.engine = engine;
  }
}





