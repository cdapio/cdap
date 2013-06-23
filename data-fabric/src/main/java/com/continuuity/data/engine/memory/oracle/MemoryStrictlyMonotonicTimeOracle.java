package com.continuuity.data.engine.memory.oracle;

import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.google.inject.Singleton;

/**
 * Implementation of timestamp oracle based on current time, ensuring no duplicates.
 */
@Singleton
public class MemoryStrictlyMonotonicTimeOracle implements TimestampOracle {

  long last = 0;

  @Override
  public synchronized long getTimestamp() {
    long cur = System.currentTimeMillis();
    if (cur <= last) {
      return ++last;
    }
    last = cur;
    return cur;
  }

}
