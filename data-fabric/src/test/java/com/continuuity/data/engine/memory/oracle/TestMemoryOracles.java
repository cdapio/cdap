package com.continuuity.data.engine.memory.oracle;

import com.continuuity.data.operation.executor.omid.TimestampOracle;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestMemoryOracles {

  @Test
  public void testMemoryStrictlyMonotonicTimeOracle() {
    TimestampOracle oracle = new MemoryStrictlyMonotonicTimeOracle();
    
    long msPrecision = 10;
    
    long now = oracle.getTimestamp();
    long curTimeMillis = System.currentTimeMillis();
    assertTrue(now - msPrecision < curTimeMillis);
    assertTrue(now + msPrecision > curTimeMillis);
    
    
    // 1000 iterations, each should be less than one millisecond
    long last = -1;
    long start = System.currentTimeMillis();
    for (int i = 0; i < 1000; i++) {
      long next = oracle.getTimestamp();
      assertTrue("Returned duplicate timestamps", last != next);
      assertTrue("Returned non-strictly-monotonic timestamps", last < next);
    }
    long end = System.currentTimeMillis();
    
    System.out.println("Time to run = " + (end - start) + "ms, " +
        "Current oracle stamp = " + oracle.getTimestamp() + ", " +
        "Current time = " + System.currentTimeMillis());
  }

}
