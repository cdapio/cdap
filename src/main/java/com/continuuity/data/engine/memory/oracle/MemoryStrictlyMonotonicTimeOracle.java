package com.continuuity.data.engine.memory.oracle;

import com.continuuity.data.operation.executor.omid.TimestampOracle;

public class MemoryStrictlyMonotonicTimeOracle implements TimestampOracle {

  long last = 0;

  @Override
  public synchronized long getTimestamp() {
    long cur = System.currentTimeMillis();
    if (cur <= last) {
      return ++last;
    }
    return last = cur;
  }

}
