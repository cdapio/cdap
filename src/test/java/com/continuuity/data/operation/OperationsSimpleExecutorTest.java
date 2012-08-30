/**
 *
 */
package com.continuuity.data.operation;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.simple.SimpleOperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.table.ColumnarTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * Simple test of operations stuff.
 */
public class OperationsSimpleExecutorTest {

  private static Injector injector;

  private ColumnarTableHandle handle;

  private OperationExecutor executor;

  @BeforeClass
  public static void initializeClass() {
    injector = Guice.createInjector(
        new DataFabricModules().getInMemoryModules());
  }
  
  @Before
  public void setUp() throws Exception {
    this.handle = injector.getInstance(ColumnarTableHandle.class);
    this.executor = new SimpleOperationExecutor(handle);
  }

  @After
  public void tearDown() throws Exception {
    this.executor = null;
  }

  @Test
  public void testSimpleReadWrite() throws Exception {
    byte [][] keys = new byte [][] { "key0".getBytes(), "key1".getBytes() };
    byte [][] values = new byte [][] {"value0".getBytes(), "value1".getBytes()};

    List<WriteOperation> writes = new ArrayList<WriteOperation>(2);
    writes.add(new Write(keys[0], values[0]));
    writes.add(new Write(keys[1], values[1]));

    this.executor.execute(writes);

    ReadKey [] reads = new ReadKey [] {
        new ReadKey(keys[0]), new ReadKey(keys[1]) };

    OperationResult<byte[]> result = this.executor.execute(reads[0]);
    assertFalse(result.isEmpty());
    assertArrayEquals(values[0], result.getValue());

    result = this.executor.execute(reads[1]);
    assertFalse(result.isEmpty());
    assertArrayEquals(values[1], result.getValue());
  }

  @Test
  public void testCompareAndSwap() throws Exception {

    byte [] key = Bytes.toBytes("somekey");

    byte [] valueOne = Bytes.toBytes("value_one");
    byte [] valueTwo = Bytes.toBytes("value_two");
    byte [] valueThree = Bytes.toBytes("value_three");

    // normal write value one
    this.executor.execute(new Write(key, valueOne));

    // CAS to Two
    this.executor.execute(new CompareAndSwap(key, valueOne, valueTwo));

    // Read normally, get valueTwo
    OperationResult<byte[]> result = this.executor.execute(new ReadKey(key));
    assertFalse(result.isEmpty());
    assertArrayEquals(valueTwo, result.getValue());

    // Bad CAS from One to Two
    try {
      this.executor.execute(new CompareAndSwap(key, valueOne, valueTwo));
      fail("Compare and swap succeeded unexpectedly.");
    } catch (OperationException e) { }

    // CAS(key, valueTwo, valueTwo)
    this.executor.execute(new CompareAndSwap(key, valueTwo, valueTwo));

    // Read normally, get valueTwo
    result = this.executor.execute(new ReadKey(key));
    assertFalse(result.isEmpty());
    assertArrayEquals(valueTwo, result.getValue());

    // CAS(key, valueTwo, valueThree)
    this.executor.execute(new CompareAndSwap(key, valueTwo, valueThree));

    // Read normally, get valueThree
    result = this.executor.execute(new ReadKey(key));
    assertFalse(result.isEmpty());
    assertArrayEquals(valueThree, result.getValue());

    // Bad CAS from null to two
    try {
      this.executor.execute(new CompareAndSwap(key, null, valueTwo));
      fail("Compare and swap succeeded unexpectedly.");
    } catch (OperationException e) { }

    // Read normally, get valueThree
    result = this.executor.execute(new ReadKey(key));
    assertFalse(result.isEmpty());
    assertArrayEquals(valueThree, result.getValue());

    // CAS from three to null
    this.executor.execute(new CompareAndSwap(key, valueThree, null));

    // Read, should not exist
    result = this.executor.execute(new ReadKey(key));
    assertTrue(result.isEmpty());
    assertNull(result.getValue());

    // CAS from null to one
    this.executor.execute(new CompareAndSwap(key, null, valueOne));

    // Read normally, get valueOne
    result = this.executor.execute(new ReadKey(key));
    assertFalse(result.isEmpty());
    assertArrayEquals(valueOne, result.getValue());

    byte [] valueChainKey = Bytes.toBytes("chainkey");
    byte [][] valueChain = generateRandomByteArrays(20, 20);

    // CompareAndSwap the first one, expecting null
    this.executor.execute(
        new CompareAndSwap(valueChainKey, null, valueChain[0]));

    // CAS down the chain
    for (int i=1; i<valueChain.length; i++) {
      this.executor.execute(
          new CompareAndSwap(valueChainKey, valueChain[i-1], valueChain[i]));
      try {
        this.executor.execute(
          new CompareAndSwap(valueChainKey, valueChain[i-1], valueChain[i]));
        fail("CompareAndSwap succeeded unexpectedly.");
      } catch (OperationException e) { // expected
      }
    }

    // Verify the current value is the last in the chain
    result = this.executor.execute(new ReadKey(valueChainKey));
    assertFalse(result.isEmpty());
    assertArrayEquals(valueChain[valueChain.length-1], result.getValue());
  }

  private long bytesToCounter(byte [] bytes) {
    if (bytes == null || bytes.length != 8) return 0L;
    return Bytes.toLong(bytes);
  }
  
  @Test
  public void testIncrement() throws Exception {


    byte [][] keys = generateRandomByteArrays(10, 8);

    // increment first half of keys by 1
    for (int i=0; i<keys.length/2; i++) {
      this.executor.execute(new Increment(keys[i], 1));
    }

    // iterate all keys, only first half should have value of 1, others 0
    for (int i=0; i<keys.length; i++) {
      long count = bytesToCounter(
          this.executor.execute(new ReadKey(keys[i])).getValue());
      if (i < keys.length/2) {
        assertEquals(1L, count);
      } else {
        assertEquals(0L, count);
      }
    }

    // decrement first half, everything should be 0
    for (int i=0; i<keys.length/2; i++) {
      this.executor.execute(new Increment(keys[i], -1));
    }

    for (byte[] key : keys) {
      assertEquals(0L, bytesToCounter(
          this.executor.execute(new ReadKey(key)).getValue()));
    }

    // increment each by their value of i
    for (int i=0; i<keys.length; i++) {
      this.executor.execute(new Increment(keys[i], i));
    }

    // read them back backwards, expecting their amount to = their position
    for (int i=keys.length-1; i>=0; i--) {
      assertEquals(i, bytesToCounter(
          this.executor.execute(new ReadKey(keys[i])).getValue()));
    }

    // increment each by the total number minus their position
    for (int i=0; i<keys.length; i++) {
      int amount = keys.length - i;
      this.executor.execute(new Increment(keys[i], amount));
    }

    // read them back, all should have the same value of keys.length
    for (byte[] key : keys) {
      assertEquals((long) keys.length, bytesToCounter(
          this.executor.execute(new ReadKey(key)).getValue()));
    }

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

  @Test
  public void testSimpleMemoryReadWrite() throws Exception {

    byte [][] keys = new byte [][] { "key0".getBytes(), "key1".getBytes() };
    byte [][] values = new byte [][] {"value0".getBytes(), "value1".getBytes()};


    SimpleOperationExecutor memoryOperationExecutor =
        new SimpleOperationExecutor(handle);

    // Client Developer : Make two write operations
    List<WriteOperation> writes = new ArrayList<WriteOperation>(2);
    writes.add(new Write(keys[0], values[0]));
    writes.add(new Write(keys[1], values[1]));

    // Runner : Execute writes through the SimpleMemoryOperationExecutor
    memoryOperationExecutor.execute(writes);
    System.out.println("Wrote two key-values");

    // Client Developer : Make two read operations
    ReadKey [] reads = new ReadKey [] {
        new ReadKey(keys[0]), new ReadKey(keys[1]) };

    // Runner : Execute reads through the SimpleMemoryOperationExecutor
    OperationResult<byte[]> result = memoryOperationExecutor.execute(reads[0]);
    assertFalse(result.isEmpty());
    assertArrayEquals(values[0], result.getValue());
    System.out.println("Read first key-value");
    result = memoryOperationExecutor.execute(reads[1]);
    assertFalse(result.isEmpty());
    assertArrayEquals(values[1], result.getValue());
    System.out.println("Read second key-value");

    assertTrue("PURPOSEFUL FAULT INJECTION!!!", true);
  }
}
