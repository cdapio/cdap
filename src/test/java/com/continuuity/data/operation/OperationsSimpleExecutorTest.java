/**
 *
 */
package com.continuuity.data.operation;

import com.continuuity.api.data.CompareAndSwap;
import com.continuuity.api.data.Increment;
import com.continuuity.api.data.OperationGenerator;
import com.continuuity.api.data.ReadKey;
import com.continuuity.api.data.ReadCounter;
import com.continuuity.api.data.Write;
import com.continuuity.api.data.WriteOperation;
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
    injector = Guice.createInjector(new DataFabricModules().getInMemoryModules());
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

    assertTrue(this.executor.execute(writes).isSuccess());

    ReadKey [] reads = new ReadKey [] {
        new ReadKey(keys[0]), new ReadKey(keys[1]) };

    byte [] value = this.executor.execute(reads[0]);
    assertEquals(new String(values[0]), new String(value));

    value = this.executor.execute(reads[1]);
    assertEquals(new String(values[1]), new String(value));
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
    assertTrue(this.executor.execute(new CompareAndSwap(key, valueOne, valueTwo)));

    // Read normally, get valueTwo
    assertTrue(Bytes.equals(valueTwo, this.executor.execute(new ReadKey(key))));

    // Bad CAS from One to Two
    assertFalse(this.executor.execute(new CompareAndSwap(key, valueOne, valueTwo)));

    // CAS(key, valueTwo, valueTwo)
    assertTrue(this.executor.execute(new CompareAndSwap(key, valueTwo, valueTwo)));

    // Read normally, get valueTwo
    assertTrue(Bytes.equals(valueTwo, this.executor.execute(new ReadKey(key))));

    // CAS(key, valueTwo, valueThree)
    assertTrue(this.executor.execute(new CompareAndSwap(key, valueTwo, valueThree)));

    // Read normally, get valueThree
    assertTrue(Bytes.equals(valueThree, this.executor.execute(new ReadKey(key))));

    // Bad CAS from null to two
    assertFalse(this.executor.execute(new CompareAndSwap(key, null, valueTwo)));

    // Read normally, get valueThree
    assertTrue(Bytes.equals(valueThree, this.executor.execute(new ReadKey(key))));

    // CAS from three to null
    assertTrue(this.executor.execute(new CompareAndSwap(key, valueThree, null)));

    // Read, should not exist
    assertNull(this.executor.execute(new ReadKey(key)));

    // CAS from null to one
    assertTrue(this.executor.execute(new CompareAndSwap(key, null, valueOne)));

    // Read normally, get valueOne
    assertTrue(Bytes.equals(valueOne, this.executor.execute(new ReadKey(key))));

    byte [] valueChainKey = Bytes.toBytes("chainkey");
    byte [][] valueChain = generateRandomByteArrays(20, 20);

    // CompareAndSwap the first one, expecting null
    assertTrue(this.executor.execute(
        new CompareAndSwap(valueChainKey, null, valueChain[0])));

    // CAS down the chain
    for (int i=1; i<valueChain.length; i++) {
      assertTrue(this.executor.execute(
          new CompareAndSwap(valueChainKey, valueChain[i-1], valueChain[i])));
      assertFalse(this.executor.execute(
          new CompareAndSwap(valueChainKey, valueChain[i-1], valueChain[i])));
    }

    // Verify the current value is the last in the chain
    assertTrue(Bytes.equals(valueChain[valueChain.length-1],
        this.executor.execute(new ReadKey(valueChainKey))));
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
      long count = this.executor.execute(new ReadCounter(keys[i]));
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

    for (int i=0; i<keys.length; i++) {
      assertEquals(0L, this.executor.execute(new ReadCounter(keys[i])));
    }

    // increment each by their value of i
    for (int i=0; i<keys.length; i++) {
      this.executor.execute(new Increment(keys[i], i));
    }

    // read them back backwards, expecting their amount to = their position
    for (int i=keys.length-1; i>=0; i--) {
      assertEquals(i, this.executor.execute(new ReadCounter(keys[i])));
    }

    // increment each by the total number minus their position
    for (int i=0; i<keys.length; i++) {
      int amount = keys.length - i;
      this.executor.execute(new Increment(keys[i], amount));
    }

    // read them back, all should have the same value of keys.length
    for (int i=0; i<keys.length; i++) {
      assertEquals(keys.length, this.executor.execute(new ReadCounter(keys[i])));
    }

  }

  @Test
  public void testIncrementChain() throws Exception {

    byte [] rawCounterKey = Bytes.toBytes("raw");
    final byte [] stepCounterKey = Bytes.toBytes("step");

    // make a generator that increments every 10 increments
    OperationGenerator<Long> generator = new OperationGenerator<Long>() {
      @Override
      public WriteOperation generateWriteOperation(Long amount) {
        if (amount % 10 == 0) return new Increment(stepCounterKey, 10);
        return null;
      }
    };

    // increment 9 times, step counter should not exist
    for (int i=0; i<9; i++) {
      Increment increment = new Increment(rawCounterKey, 1);
      increment.setPostIncrementOperationGenerator(generator);
      assertTrue(this.executor.execute(increment));
      assertEquals(new Long(i+1), increment.getResult());
    }

    // raw should be 9, step should be 0
    assertEquals(9L, this.executor.execute(new ReadCounter(rawCounterKey)));
    assertEquals(0L, this.executor.execute(new ReadCounter(stepCounterKey)));

    // one more and raw should be 10, step should be 1
    Increment increment = new Increment(rawCounterKey, 1);
    increment.setPostIncrementOperationGenerator(generator);
    assertTrue(this.executor.execute(increment));
    assertEquals(10L, this.executor.execute(new ReadCounter(rawCounterKey)));
    assertEquals(10L, this.executor.execute(new ReadCounter(stepCounterKey)));

    // 15 more increments
    for (int i=0; i<15; i++) {
      increment = new Increment(rawCounterKey, 1);
      increment.setPostIncrementOperationGenerator(generator);
      assertTrue(this.executor.execute(increment));
      assertEquals(new Long(i+11), increment.getResult());
    }
    // raw should be 25, step should be 20
    assertEquals(25L, this.executor.execute(new ReadCounter(rawCounterKey)));
    assertEquals(20L, this.executor.execute(new ReadCounter(stepCounterKey)));
  }

  @Test
  public void testReadModifyWrite() throws Exception {

    byte [] key = Bytes.toBytes("keyrmw");
    byte [] value = Bytes.toBytes(0L);

    // write the first value (0)
    assertTrue(this.executor.execute(new Write(key, value)));

    // create two modifiers.  an incrementer and decrementer.
    Modifier<byte[]> incrementer = new Modifier<byte[]>() {
      @Override
      public byte [] modify(byte [] bytes) {
        return Bytes.toBytes(Bytes.toLong(bytes)+1);
      }
    };
    Modifier<byte[]> decrementer = new Modifier<byte[]>() {
      @Override
      public byte [] modify(byte [] bytes) {
        return Bytes.toBytes(Bytes.toLong(bytes)-1);
      }
    };

    // increment 10 times
    for (int i=0; i<10; i++)
      assertTrue(this.executor.execute(new ReadModifyWrite(key, incrementer)));

    // verify value is 10L
    assertEquals(10L, Bytes.toLong(this.executor.execute(new ReadKey(key))));

    // decrement 12 times
    for (int i=0; i<12; i++)
      assertTrue(this.executor.execute(new ReadModifyWrite(key, decrementer)));

    // verify value is -2L
    assertEquals(-2L, Bytes.toLong(this.executor.execute(new ReadKey(key))));

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
    assertTrue(memoryOperationExecutor.execute(writes).isSuccess());
    System.out.println("Wrote two key-values");

    // Client Developer : Make two read operations
    ReadKey [] reads = new ReadKey [] {
        new ReadKey(keys[0]), new ReadKey(keys[1]) };

    // Runner : Execute reads through the SimpleMemoryOperationExecutor
    byte [] value = memoryOperationExecutor.execute(reads[0]);
    assertEquals(new String(values[0]), new String(value));
    System.out.println("Read first key-value");
    value = memoryOperationExecutor.execute(reads[1]);
    assertEquals(new String(values[1]), new String(value));
    System.out.println("Read second key-value");

    assertTrue("PURPOSEFUL FAULT INJECTION!!!", true);
  }
}
