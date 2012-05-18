/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.engine.memory;

import static org.junit.Assert.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestMemoryOmidTransactionEngine {

  @Test
  public void testSimple() throws Exception {
    
    MemoryTransactionalEngine txEngine = new MemoryTransactionalEngine();
    MemoryOmidTransactionEngine omidEngine =
        new MemoryOmidTransactionEngine(txEngine);
    
    byte [] key = Bytes.toBytes("key");
    byte [] value = Bytes.toBytes("value");
    
    // start a transaction
    long txid = omidEngine.startTransaction();
    
    // write to a key
    omidEngine.write(key, value, txid);
    
    // read should see nothing
    assertNull(omidEngine.read(key));
    
    // commit
    assertTrue(omidEngine.commitTransaction(txid));
    
    // read should see the write
    byte [] readValue = omidEngine.read(key);
    assertNotNull(readValue);
    assertTrue(Bytes.equals(readValue, value));
  }

  @Test
  public void testConcurrentWrites() throws Exception {
    MemoryTransactionalEngine txEngine = new MemoryTransactionalEngine();
    MemoryOmidTransactionEngine omidEngine =
        new MemoryOmidTransactionEngine(txEngine);
    
    byte [] key = Bytes.toBytes("key");
    byte [] valueOne = Bytes.toBytes("value1");
    byte [] valueTwo = Bytes.toBytes("value2");
    
    // start tx one
    long txidOne = omidEngine.startTransaction();
    System.out.println("Started transaction one : " + txidOne);
    
    // write value one
    omidEngine.write(key, valueOne, txidOne);
    
    // read should see nothing
    assertNull(omidEngine.read(key));
    
    // start tx two
    long txidTwo = omidEngine.startTransaction();
    System.out.println("Started transaction two : " + txidTwo);
    assertTrue(txidTwo > txidOne);
    
    // write value two
    omidEngine.write(key, valueTwo, txidTwo);
    
    // read should see nothing
    assertNull(omidEngine.read(key));
    
    // commit tx two, should succeed
    assertTrue(omidEngine.commitTransaction(txidTwo));
    
    // even though committed, tx one still not complete so read
    // is not visible!
    byte [] readValue = omidEngine.read(key);
    assertNull(readValue);
    
    // commit tx one, should fail
    assertFalse(omidEngine.commitTransaction(txidOne));
    
    // but now tx one is committed, read point should move and
    // we should see tx two
    readValue = omidEngine.read(key);
    assertNotNull(readValue);
    assertTrue(Bytes.equals(readValue, valueTwo));
  }
}
