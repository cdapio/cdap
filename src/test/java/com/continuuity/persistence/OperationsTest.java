/**
 * 
 */
package com.continuuity.persistence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.continuuity.fabric.engine.memory.MemoryEngine;
import com.continuuity.fabric.engine.memory.MemorySimpleExecutor;
import com.continuuity.fabric.operations.SimpleOperationExecutor;
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
	public void testAllOperationsAgainstSimpleExecutor() {
    MemoryEngine memoryEngine = new MemoryEngine();
    MemorySimpleExecutor memoryExecutor =
        new MemorySimpleExecutor(memoryEngine);
    MemorySimpleOperationExecutor memoryOperationExecutor =
        new MemorySimpleOperationExecutor(memoryExecutor);
    testOperations(memoryOperationExecutor);
	}

	private void testOperations(SimpleOperationExecutor exec) {
	  testSimpleReadWrite(exec);
	  testOrderedReadWrite(exec);
	  testReadModifyWrite(exec);
	  testIncrement(exec);
	  testCompareAndSwap(exec);
	  testQueues(exec);
	}

  private void testSimpleReadWrite(SimpleOperationExecutor exec) {
    // TODO Auto-generated method stub
    
  }

  private void testOrderedReadWrite(SimpleOperationExecutor exec) {
    // TODO Auto-generated method stub
    
  }

  private void testQueues(SimpleOperationExecutor exec) {
    // TODO Auto-generated method stub
    
  }

  private void testCompareAndSwap(SimpleOperationExecutor exec) {
    // TODO Auto-generated method stub
    
  }

  private void testIncrement(SimpleOperationExecutor exec) {
    // TODO Auto-generated method stub
    
  }

  private void testReadModifyWrite(SimpleOperationExecutor exec) {
    // TODO Auto-generated method stub
    
  }
}
