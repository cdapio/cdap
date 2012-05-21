/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric;

import static org.junit.Assert.fail;

import org.junit.Test;

import com.continuuity.data.operation.executor.TransactionalOperationExecutor;
import com.continuuity.fabric.deadpool.MemoryTransactionalOperationExecutor;

/**
 * Test that mimics the level of access/behavior of the fabric from flows.
 * 
 * This uses transactional stuff!
 */
public class TestLikeAFlow {

  @Test
  public void testMemorySetupAndTeardown() {

//    // Use the in-memory 
//    TransactionalOperationExecutor executor =
//        new MemoryTransactionalOperationExecutor();
    
    fail("Not yet implemented");
  }

}
