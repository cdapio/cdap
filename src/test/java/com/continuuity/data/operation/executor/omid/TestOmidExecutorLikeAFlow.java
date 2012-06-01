package com.continuuity.data.operation.executor.omid;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryOracle;

public class TestOmidExecutorLikeAFlow {
  
  private final TimestampOracle timeOracle = new MemoryStrictlyMonotonicOracle();
  @SuppressWarnings("unused")
  private final TransactionOracle oracle = new MemoryOracle(timeOracle);
  @SuppressWarnings("unused")
  private final Configuration conf = new Configuration();
  
  @Test
  public void testStandaloneDequeue() throws Exception {
    
  }
  
  @Test
  public void testWriteBatchJustAck() throws Exception {
    
  }
  
  @Test
  public void testWriteBatchWithMultiWritesMultiEnqueuesPlusSuccessfulAck()
  throws Exception {
    
    // Verify it reorders queue ops appropriately and does a stable sort
    // of normal writes
  }

 
  @Test
  public void testWriteBatchWithMultiWritesMultiEnqueuesPlusUnsuccessfulAckRollback()
  throws Exception {
    
  }
}
