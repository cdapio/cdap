package com.continuuity.data.engine.memory.oracle;

import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.google.inject.Singleton;

/**
 * Implementation of timestamp oracle based on a counter.
 */
@Singleton
public class MemoryStrictlyMonotonicOracle implements TimestampOracle {

  private long last = 0;

  @Override
  public synchronized long getTimestamp() {
    return ++last;
  }

}
