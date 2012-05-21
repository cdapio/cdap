/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.engine.memory;

import static org.junit.Assert.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.continuuity.fabric.deadpool.MemoryTransactionalEngine;
import com.continuuity.fabric.deadpool.MemoryTransactionalExecutor.TransactionException;

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
  public void testOverlappingConcurrentWrites() throws Exception {
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
    
    // even though tx one not committed, we can see two already
    byte [] readValue = omidEngine.read(key);
    assertNotNull(readValue);
    assertTrue(Bytes.equals(readValue, valueTwo));
    
    // commit tx one, should fail
    assertFalse(omidEngine.commitTransaction(txidOne));
    
    // should still see two
    readValue = omidEngine.read(key);
    assertNotNull(readValue);
    assertTrue(Bytes.equals(readValue, valueTwo));
  }

  @Test
  public void testClosedTransactionsThrowExceptions() throws Exception {
    MemoryTransactionalEngine txEngine = new MemoryTransactionalEngine();
    MemoryOmidTransactionEngine omidEngine =
        new MemoryOmidTransactionEngine(txEngine);
    
    byte [] key = Bytes.toBytes("key");
    
    // start txwOne
    long txwOne = omidEngine.startTransaction();
    System.out.println("Started transaction txwOne : " + txwOne);
    
    // write and commit
    omidEngine.write(key, Bytes.toBytes(1), txwOne);
    assertTrue(omidEngine.commitTransaction(txwOne));
    
    // trying to write with this tx should throw exception
    try {
      omidEngine.write(key, Bytes.toBytes(2), txwOne);
      fail("Writing with committed transaction should throw exception");
    } catch (TransactionException te) {
      // correct
    }
    
    // trying to commit this tx should throw exception
    try {
      omidEngine.commitTransaction(txwOne);
      fail("Committing with committed transaction should throw exception");
    } catch (TransactionException te) {
      // correct
    }
    
    // read should see value 1 not 2
    assertTrue(
        Bytes.equals(omidEngine.read(key), Bytes.toBytes(1)));
  }

  @Test
  public void testOverlappingConcurrentReadersAndWriters() throws Exception {
    MemoryTransactionalEngine txEngine = new MemoryTransactionalEngine();
    MemoryOmidTransactionEngine omidEngine =
        new MemoryOmidTransactionEngine(txEngine);
    
    byte [] key = Bytes.toBytes("key");
    
    // start txwOne
    long txwOne = omidEngine.startTransaction();
    System.out.println("Started transaction txwOne : " + txwOne);
    
    // write 1
    omidEngine.write(key, Bytes.toBytes(1), txwOne);
    
    // read should see nothing
    assertNull(omidEngine.read(key));
    
    // commit write 1
    omidEngine.commitTransaction(txwOne);
    
    // read sees 1
    assertTrue(
        Bytes.equals(omidEngine.read(key), Bytes.toBytes(1)));
    
    // open long-running read
    long txrOne = omidEngine.startTransaction();
    
    // write 2 and commit immediately
    long txwTwo = omidEngine.startTransaction();
    System.out.println("Started transaction txwTwo : " + txwTwo);
    omidEngine.write(key, Bytes.toBytes(2), txwTwo);
    assertTrue(omidEngine.commitTransaction(txwTwo));
    
    // read sees 2
    byte [] value = omidEngine.read(key);
    assertNotNull(value);
    System.out.println("Value is : " + value.length + ", " + Bytes.toInt(value));
    assertTrue("expect 2, actually " + Bytes.toInt(value),
        Bytes.equals(omidEngine.read(key), Bytes.toBytes(2)));
    
    // open long-running read
    long txrTwo = omidEngine.startTransaction();
    
    // write 3 with one transaction but don't commit
    long txwThree = omidEngine.startTransaction();
    System.out.println("Started transaction txwThree : " + txwThree);
    omidEngine.write(key, Bytes.toBytes(3), txwThree);
    
    // write 4 with another transaction and also don't commit
    long txwFour = omidEngine.startTransaction();
    System.out.println("Started transaction txwFour : " + txwFour);
    omidEngine.write(key, Bytes.toBytes(4), txwFour);
    
    // read sees 2 still
    assertTrue(
        Bytes.equals(omidEngine.read(key), Bytes.toBytes(2)));
    
    // commit 4, should be successful
    assertTrue(omidEngine.commitTransaction(txwFour));
    
    // read sees 4
    assertTrue(
        Bytes.equals(omidEngine.read(key), Bytes.toBytes(4)));
    
    // commit 3, should fail
    assertFalse(omidEngine.commitTransaction(txwThree));
    
    // read still sees 4
    assertTrue(
        Bytes.equals(omidEngine.read(key), Bytes.toBytes(4)));
    
    // now read with long-running read 1, should see value = 1
    assertTrue(
        Bytes.equals(omidEngine.read(key, txrOne), Bytes.toBytes(1)));
    
    // now do the same thing but in reverse order of conflict
    
    // write 5 with one transaction but don't commit
    long txwFive = omidEngine.startTransaction();
    System.out.println("Started transaction txwFive : " + txwFive);
    omidEngine.write(key, Bytes.toBytes(5), txwFive);
    
    // write 6 with another transaction and also don't commit
    long txwSix = omidEngine.startTransaction();
    System.out.println("Started transaction txwSix : " + txwSix);
    omidEngine.write(key, Bytes.toBytes(6), txwSix);
    
    // read sees 4 still
    assertTrue(
        Bytes.equals(omidEngine.read(key), Bytes.toBytes(4)));
    
    // long running reads should still see their respective values
    assertTrue(
        Bytes.equals(omidEngine.read(key, txrOne), Bytes.toBytes(1)));
    assertTrue(
        Bytes.equals(omidEngine.read(key, txrTwo), Bytes.toBytes(2)));
    
    // commit 5, should be successful
    assertTrue(omidEngine.commitTransaction(txwFive));
    
    // read sees 5
    assertTrue(
        Bytes.equals(omidEngine.read(key), Bytes.toBytes(5)));
    
    // long running reads should still see their respective values
    assertTrue(
        Bytes.equals(omidEngine.read(key, txrOne), Bytes.toBytes(1)));
    assertTrue(
        Bytes.equals(omidEngine.read(key, txrTwo), Bytes.toBytes(2)));
    
    // commit 6, should fail
    assertFalse(omidEngine.commitTransaction(txwSix));
    
    // read still sees 5
    assertTrue(
        Bytes.equals(omidEngine.read(key), Bytes.toBytes(5)));
    
    // long running reads should still see their respective values
    assertTrue(
        Bytes.equals(omidEngine.read(key, txrOne), Bytes.toBytes(1)));
    assertTrue(
        Bytes.equals(omidEngine.read(key, txrTwo), Bytes.toBytes(2)));
  }
}
