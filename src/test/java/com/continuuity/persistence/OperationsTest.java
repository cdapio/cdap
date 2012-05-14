/**
 * 
 */
package com.continuuity.persistence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.continuuity.fabric.engine.memory.MemoryEngine;
import com.continuuity.fabric.engine.memory.MemorySimpleExecutor;
import com.continuuity.fabric.operations.SimpleOperationExecutor;
import com.continuuity.fabric.operations.impl.CompareAndSwap;
import com.continuuity.fabric.operations.impl.Read;
import com.continuuity.fabric.operations.impl.Write;
import com.continuuity.fabric.operations.memory.MemorySimpleOperationExecutor;

/**
 * Simple test of operations stuff.
 */
public class OperationsTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSimpleMemoryReadWrite() throws Exception {

	  byte [][] keys = new byte [][] { "key0".getBytes(), "key1".getBytes() };
	  byte [][] values = new byte [][] {"value0".getBytes(), "value1".getBytes()};

	  // Fabric : We are using a MemoryEngine and its NativeSimpleExecutor
	  MemoryEngine memoryEngine = new MemoryEngine();
	  MemorySimpleExecutor memoryExecutor =
	      new MemorySimpleExecutor(memoryEngine);

	  // Runner : Create Memory SimpleOperationExecutor using NativeMemorySimpExec
    MemorySimpleOperationExecutor memoryOperationExecutor =
        new MemorySimpleOperationExecutor(memoryExecutor);

	  // Client Developer : Make two write operations
	  Write [] writes = new Write [] {
	      new Write(keys[0], values[0]), new Write(keys[1], values[1]) };

	  // Runner : Execute writes through the SimpleMemoryOperationExecutor
	  assertTrue(memoryOperationExecutor.execute(writes));
	  System.out.println("Wrote two key-values");

	  // Client Developer : Make two read operations
	  Read [] reads = new Read [] {
	      new Read(keys[0]), new Read(keys[1]) };

	  // Runner : Execute reads through the SimpleMemoryOperationExecutor
	  byte [] value = memoryOperationExecutor.execute(reads[0]);
	  assertEquals(new String(values[0]), new String(value));
	  System.out.println("Read first key-value");
	  value = memoryOperationExecutor.execute(reads[1]);
	  assertEquals(new String(values[1]), new String(value));
	  System.out.println("Read second key-value");
	  
	  assertTrue("PURPOSEFUL FAULT INJECTION!!!", true);
	}

	@Test
	public void testAllOperationsAgainstSimpleMemoryExecutor() throws Exception {
    MemoryEngine memoryEngine = new MemoryEngine();
    MemorySimpleExecutor memoryExecutor =
        new MemorySimpleExecutor(memoryEngine);
    MemorySimpleOperationExecutor memoryOperationExecutor =
        new MemorySimpleOperationExecutor(memoryExecutor);
    testOperations(memoryOperationExecutor);
	}

	private void testOperations(SimpleOperationExecutor exec) throws Exception {
	  testSimpleReadWrite(exec);
	  testOrderedReadWrite(exec);
	  testReadModifyWrite(exec);
	  testIncrement(exec);
	  testCompareAndSwap(exec);
	  testQueues(exec);
	}

  private void testSimpleReadWrite(SimpleOperationExecutor exec)
  throws Exception {
    byte [][] keys = new byte [][] { "key0".getBytes(), "key1".getBytes() };
    byte [][] values = new byte [][] {"value0".getBytes(), "value1".getBytes()};

    Write [] writes = new Write [] {
        new Write(keys[0], values[0]), new Write(keys[1], values[1]) };

    assertTrue(exec.execute(writes));

    Read [] reads = new Read [] {
        new Read(keys[0]), new Read(keys[1]) };

    byte [] value = exec.execute(reads[0]);
    assertEquals(new String(values[0]), new String(value));
    
    value = exec.execute(reads[1]);
    assertEquals(new String(values[1]), new String(value));
  }

  private void testCompareAndSwap(SimpleOperationExecutor exec)
  throws Exception {
    
    byte [] key = Bytes.toBytes("somekey");
    
    byte [] valueOne = Bytes.toBytes("value_one");
    byte [] valueTwo = Bytes.toBytes("value_two");
    byte [] valueThree = Bytes.toBytes("value_three");
    
    // normal write value one
    exec.execute(new Write(key, valueOne));
    
    // CAS to Two
//    assertTrue(exec.execute(new CompareAndSwap(key, valueOne, valueTwo)));
//    
//    // Read normally, get valueTwo
//    assertTrue(Bytes.equals(valueTwo, exec.execute(new Read(key))));
//    
//    // Bad CAS from One to Two
//    assertFalse(exec.execute(new CompareAndSwap(key, valueOne, valueTwo)));
//    
//    // CAS(key, valueTwo, valueTwo)
//    assertTrue(exec.execute(new CompareAndSwap(key, valueTwo, valueTwo)));
//    
//    // Read normally, get valueTwo
//    assertTrue(Bytes.equals(valueTwo, exec.execute(new Read(key))));
//    
//    // CAS(key, valueTwo, valueThree)
//    assertTrue(exec.execute(new CompareAndSwap(key, valueTwo, valueThree)));
//    
    // Read normally, get valueThree
    // assertEquals(valueThree, read(key))
    
    // Bad CAS from null to two
    // CAS(key, null, valueTwo)
    // assertFalse
    
    // Read normally, get valueThree
    // assertEquals(valueThree, read(key))
    
    // CAS from three to null
    // CAS(key, valueThree, null)
    // assertTrue
    
    // Read, should not exist
    // assertNull(read(key))
    
    // CAS from null to one
    // CAS(key, null, valueOne)

    // Read normally, get valueOne
    // assertEquals(valueOne, read(key))
    
    
    byte [] valueChainKey = Bytes.toBytes("chainkey");
    byte [][] valueChain = generateRandomByteArrays(20, 20); 
    
    // CompareAndSwap the first one, expecting null
    
    // CAS(valueChainKey, null, valueChain[0])
    
    // CAS down the chain
    for (int i=1; i<valueChain.length; i++) {
      // CAS(valueChainKey, valueChain[i-1], valueChain[i])
      // assert true
      // attempt CAS(valueChainKey, valueChain[i-1], valueChain[i])
      // assert false
    }
    
    // Verify the current value is the last in the chain
    // assert equals (valueChain[valueChain.length-1], read(valueChainKey))
    
  }

  private void testIncrement(SimpleOperationExecutor exec) {

    
    byte [][] keys = generateRandomByteArrays(10, 8);
    
    // increment first half of keys by 1
    for (int i=0; i<keys.length/2; i++) {
      // increment keys[i]
    }
    
    // iterate all keys, only first half should have value of 1, others 0
    for (int i=0; i<keys.length; i++) {
      // read keys[i]
      if (i < keys.length/2) {
        // assertEquals(1, keys[i])
      } else {
        // assertEquals(0, keys[i])
      }
    }
    
    // decrement first half, everything should be 0
    for (int i=0; i<keys.length/2; i++) {
      // decrement keys[i]
    }

    for (int i=0; i<keys.length; i++) {
      // assertEquals(0, keys[i])
    }
    
    // increment each by their value of i
    for (int i=0; i<keys.length; i++) {
      // increment(keys[i], i)
    }
    
    // read them back backwards, expecting their amount to = their position
    for (int i=keys.length-1; i>=0; i--) {
      // assertEquals(i, keys[i])
    }
    
    // increment each by the total number minus their position
    for (int i=0; i<keys.length; i++) {
      int amount = keys.length - i;
      // increment(keys[i], amount)
    }
    
    // read them back, all should have the same value of keys.length
    for (int i=0; i<keys.length; i++) {
      // assertEquals(keys.length, keys[i])
    }
    
  }

  private void testReadModifyWrite(SimpleOperationExecutor exec) {
    // TODO Implement read-modify-write test
    
  }

  private void testOrderedReadWrite(SimpleOperationExecutor exec) {
    // TODO Implement ordered read-write test
  }

  private void testQueues(SimpleOperationExecutor exec) {
    // TODO Implement queue test
  }
  
  // Private helpers

  private static final Random rand = new Random();

  private byte[][] generateRandomByteArrays(int num, int length) {
    byte [][] bytes = new byte[num][];
    for (int i=0;i<num;i++) {
      bytes[i] = new byte[length];
      rand.nextBytes(bytes[i]);
    }
    return bytes;
  }
  
}
