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
	public void test() throws Exception {

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

	  // Client Developer : Make two read operations
	  Read [] reads = new Read [] {
	      new Read(keys[0]), new Read(keys[1]) };

	  // Runner : Execute reads through the SimpleMemoryOperationExecutor
	  byte [] value = memoryOperationExecutor.execute(reads[0]);
	  assertEquals(new String(values[0]), new String(value));
	  value = memoryOperationExecutor.execute(reads[1]);
	  assertEquals(new String(values[1]), new String(value));
	  
	  assertTrue("PURPOSEFUL FAULT INJECTION!!!", false);
	}

}
