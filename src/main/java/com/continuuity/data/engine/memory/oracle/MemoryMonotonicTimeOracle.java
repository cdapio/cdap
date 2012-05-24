package com.continuuity.data.engine.memory.oracle;

import com.continuuity.data.operation.executor.omid.TimestampOracle;

public class MemoryMonotonicTimeOracle implements TimestampOracle {

  @Override
  public long getTimestamp() {
    return System.currentTimeMillis();
  }

}
