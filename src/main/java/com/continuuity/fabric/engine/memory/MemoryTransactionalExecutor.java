package com.continuuity.fabric.engine.memory;

import com.continuuity.fabric.engine.NativeTransactionalExecutor;

public class MemoryTransactionalExecutor implements NativeTransactionalExecutor {

  @SuppressWarnings("unused")
  private final MemorySimpleEngine engine;

  public MemoryTransactionalExecutor(MemorySimpleEngine engine) {
    this.engine = engine;
  }
}





