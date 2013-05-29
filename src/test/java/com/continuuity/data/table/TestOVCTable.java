package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests the contract and semantics of {@link OrderedVersionedColumnarTable}
 * against each of the implementations.
 */
public abstract class TestOVCTable {

  protected OrderedVersionedColumnarTable table;
  protected OVCTableHandle tableHandle;

  protected static final Random r = new Random();

  protected OVCTableHandle getTableHandle() {
    return this.tableHandle;
  }

  @Before
  public void initialize() throws OperationException {
    System.out.println("\n\nBeginning test\n\n");
    this.tableHandle = injectTableHandle();
    this.table = this.tableHandle.getTable(Bytes.toBytes("TestOVCTable" + Math.abs(r.nextInt())));
  }

  protected abstract OVCTableHandle injectTableHandle();

  protected static final byte [] COL = new byte [] { (byte)0 };
  protected static final MemoryReadPointer RP_MAX = new MemoryReadPointer(Long.MAX_VALUE);

  /**
   * Every subclass should implement this to verify that injection works and uses the correct table type
   */
  @Test
  public abstract void testInjection();

  @Test
  public void testSimpleReadWrite() throws OperationException {

    byte [] row = Bytes.toBytes("testSimpleReadWrite");

    this.table.put(row, COL, 1L, row);

    assertEquals(Bytes.toString(row),
        Bytes.toString(this.table.get(row, COL, RP_MAX).getValue()));
    assertEquals(Bytes.toString(row), Bytes.toString(
            this.table.get(row, COL, new MemoryReadPointer(1L)).getValue()));

  }

  @Test
  public void testClearVerySimply() throws OperationException {
    byte [] row = Bytes.toBytes("testClear");

    assertTrue(this.table.get(row, COL, RP_MAX).isEmpty());

    this.table.put(row, COL, 1L, row);

    assertEquals(Bytes.toString(row),
        Bytes.toString(this.table.get(row, COL, RP_MAX).getValue()));

    this.table.clear();

    assertTrue(this.table.get(row, COL, RP_MAX).isEmpty());

    this.table.put(row, COL, 1L, row);

    assertEquals(Bytes.toString(row),
        Bytes.toString(this.table.get(row, COL, RP_MAX).getValue()));

  }

  @Test
  public void testMultiColumnReadsAndWrites() throws OperationException {

    byte [] row = Bytes.toBytes("testMultiColumnReadsAndWrites");

    int ncols = 100;
    assertTrue(ncols % 2 == 0); // needs to be even in this test
    byte [][] columns = new byte[ncols][];
    for (int i=0;i<ncols;i++) {
      columns[i] = Bytes.toBytes(new Long(i));
    }
    byte [][] values = new byte[ncols][];
    for (int i=0;i<ncols;i++) values[i] = Bytes.toBytes(Math.abs(r.nextLong()));

    // insert a version of every column, two at a time
    long version = 10L;
    for (int i=0;i<ncols;i+=2) {
      this.table.put(row, new byte [][] {columns[i], columns[i+1]}, version, new byte [][] {values[i], values[i+1]});
    }

    // read them all back at once using all the various read apis

    // get(row)
    Map<byte[],byte[]> colMap = this.table.get(row, RP_MAX).getValue();
    assertEquals(ncols, colMap.size());
    int idx=0;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,col)
    for(int i=0;i<ncols;i++) {
      byte [] value = this.table.get(row, columns[i], RP_MAX).getValue();
      assertTrue(Bytes.equals(value, values[i]));
    }

    // getWV(row,col)
    for(int i=0;i<ncols;i++) {
      ImmutablePair<byte[],Long> valueAndVersion = this.table.getWithVersion(row, columns[i], RP_MAX).getValue();
      assertTrue(Bytes.equals(valueAndVersion.getFirst(), values[i]));
      assertEquals(new Long(version), valueAndVersion.getSecond());
    }

    // get(row,startCol,stopCol)

    // get(row,start=null,stop=null,unlimited)
    colMap = this.table.get(row, null, null, -1, RP_MAX).getValue();
    assertEquals(ncols, colMap.size());
    idx=0;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,start=0,stop=ncols+1, limit)
    colMap = this.table.get(row, Bytes.toBytes((long)0),
        Bytes.toBytes((long)ncols+1), -1, RP_MAX).getValue();
    assertEquals(ncols, colMap.size());
    idx=0;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,cols[ncols])
    colMap = this.table.get(row, columns, RP_MAX).getValue();
    assertEquals(ncols, colMap.size());
    idx=0;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,cols[ncols-2])
    byte [][] subCols = Arrays.copyOfRange(columns, 1, ncols - 1);
    colMap = this.table.get(row, subCols, RP_MAX).getValue();
    assertEquals(ncols - 2, colMap.size());
    idx=1;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,RP=9) = 0 cols
    assertTrue(this.table.get(row, new MemoryReadPointer(9)).isEmpty());

    // delete the first 5 as point deletes
    subCols = Arrays.copyOfRange(columns, 0, 5);
    this.table.delete(row, subCols, version);

    // get returns 5 less
    colMap = this.table.get(row, RP_MAX).getValue();
    assertEquals(ncols - 5, colMap.size());

    // delete the second 5 as delete alls
    subCols = Arrays.copyOfRange(columns, 5, 10);
    this.table.deleteAll(row, subCols, version);

    // get returns 10 less
    colMap = this.table.get(row, RP_MAX).getValue();
    assertEquals(ncols - 10, colMap.size());

    // delete the third 5 as delete alls
    subCols = Arrays.copyOfRange(columns, 10, 15);
    this.table.deleteAll(row, subCols, version);

    // get returns 15 less
    colMap = this.table.get(row, RP_MAX).getValue();
    assertEquals(ncols - 15, colMap.size());

    // undelete the second 5 with old code
    // with new code, undeleteAll would not restore puts from same transation !!
    // so, with new code
    subCols = Arrays.copyOfRange(columns, 5, 10);
    this.table.undeleteAll(row, subCols, version);

    // get returns 10 less for old code
    // get returns 15 less for new code
    colMap = this.table.get(row, RP_MAX).getValue();
    assertEquals(ncols - 10, colMap.size());
    //assertEquals(ncols - 15, colMap.size());
  }

  @Test
  public void testReadColumnRange() throws OperationException {

    byte [] row = Bytes.toBytes("testReadColumnRange");

    int ncols = 100;
    assertTrue(ncols % 2 == 0); // needs to be even in this test
    byte [][] columns = new byte[ncols][];
    for (int i=0;i<ncols;i++) {
      columns[i] = Bytes.toBytes(new Long(i));
    }
    byte [][] values = new byte[ncols][];
    for (int i=0;i<ncols;i++) values[i] = Bytes.toBytes(Math.abs(r.nextLong()));

    // insert a version of every column, two at a time
    long version = 10L;
    for (int i=0;i<ncols;i+=2) {
      this.table.put(row, new byte [][] { columns[i], columns[i+1] }, version,
          new byte [][] { values[i], values[i+1] });
    }

    // get(row,startCol,stopCol)
    // get(row,start=null,stop=null, unlimited)
    Map<byte[],byte[]> colMap =
        this.table.get(row, null, null, -1, RP_MAX).getValue();
    assertEquals(ncols, colMap.size());
    int idx=0;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,start=0,stop=ncols+1, unlimited)
    colMap = this.table.get(row, Bytes.toBytes((long)0),
        Bytes.toBytes((long)ncols+1), -1, RP_MAX).getValue();
    assertEquals(ncols, colMap.size());
    idx=0;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,start=0,stop=ncols+1,limit=12)
    colMap = this.table.get(row, Bytes.toBytes((long)0),
        Bytes.toBytes((long)ncols+1), 12, RP_MAX).getValue();
    assertEquals(12, colMap.size());
    idx=0;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,start=1,stop=ncols-1, unlimited) = ncols-2
    colMap = this.table.get(row, Bytes.toBytes((long)1),
        Bytes.toBytes((long)ncols-1), -1, RP_MAX).getValue();
    assertEquals(ncols - 2, colMap.size());
    idx=1;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,start=10,stop=20, unlimited) = 10
    colMap = this.table.get(row, Bytes.toBytes((long)10),
        Bytes.toBytes((long)20), -1, RP_MAX).getValue();
    assertEquals(10, colMap.size());
    idx=10;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,start=10,stop=20,limit=5) = 5
    colMap = this.table.get(row, Bytes.toBytes((long)10),
        Bytes.toBytes((long)20), 5, RP_MAX).getValue();
    assertEquals(5, colMap.size());
    idx=10;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

  }

  @Test
  public void testColumnRangeWithVersions() throws OperationException {
    byte [] row = Bytes.toBytes("testColumnRangeWithVersions");
    byte [] one = { 1 };
    byte [] two = { 2 };
    byte [] three = { 3 };
    byte [] colA = { 'a' };
    byte [] colB = { 'b' };
    byte [] colC = { 'c' };
    byte [] colD = { 'd' };

    // write 1 to each of four columns with version 10
    this.table.put(row, new byte [][] { colA, colB, colC, colD }, 10L,
        new byte [][] { one, one, one, one });
    // write 2 to two of the four columns with version 11
    this.table.put(row, new byte [][] { colB, colC }, 11L,
        new byte [][] { two, two });
    // write 3 to one of these two columns with version 12
    this.table.put(row, new byte [][] { colC }, 12L,
        new byte [][] { three });

    // read all columns [A..D[ with latest version and no exclusions
    Map<byte[],byte[]> colMap =
        this.table.get(row, colA, colD, -1, RP_MAX).getValue();
    Assert.assertArrayEquals(one, colMap.get(colA));
    Assert.assertArrayEquals(two, colMap.get(colB));
    Assert.assertArrayEquals(three, colMap.get(colC));
    Assert.assertNull(colMap.get(colD));

    // read columns [B..] with version <= 11 and no exclusions
    colMap = this.table.
        get(row, colB, null, -1, new MemoryReadPointer(11)).getValue();
    Assert.assertNull(colMap.get(colA));
    Assert.assertArrayEquals(two, colMap.get(colB));
    Assert.assertArrayEquals(two, colMap.get(colC));
    Assert.assertArrayEquals(one, colMap.get(colD));

    // read all columns with version <= 12 and exclusions is 11
    colMap = this.table.get(row, null, null, -1,
        new MemoryReadPointer(12, Collections.singleton(11L))).getValue();
    Assert.assertArrayEquals(one, colMap.get(colA));
    Assert.assertArrayEquals(one, colMap.get(colB));
    Assert.assertArrayEquals(three, colMap.get(colC));
    Assert.assertArrayEquals(one, colMap.get(colD));
  }

  @Test
  public void testSimpleIncrement() throws OperationException {

    byte [] row = Bytes.toBytes("testSimpleIncrement");

    assertEquals(1L, this.table.increment(row, COL, 1L, RP_MAX, 1L));
    assertEquals(3L, this.table.increment(row, COL, 2L, RP_MAX, 2L));
    assertEquals(3L, Bytes.toLong(this.table.get(row, COL, RP_MAX).getValue()));

  }

  @Test
  public void testMultiColumnIncrement() throws OperationException {

    byte [] row = Bytes.toBytes("testMultiColumnIncrement");

    int ncols = 100;
    assertTrue(ncols % 2 == 0); // needs to be even in this test
    byte [][] columns = new byte[ncols][];
    for (int i=0;i<ncols;i++) {
      columns[i] = Bytes.toBytes(new Long(i));
    }

    // increment the evens individually
    long version = 10;
    for (int i=0; i<ncols; i+=2) {
      assertEquals(1L,
          this.table.increment(row, columns[i], 1, RP_MAX, version));
    }

    // increment everything at once
    long [] amounts = new long[ncols];
    for (int i=0;i<ncols;i++) amounts[i] = (long)i+1;
    Map<byte[],Long> counters =
        this.table.increment(row, columns, amounts, RP_MAX, version);
    assertEquals(ncols, counters.size());
    int idx = 0;
    for (Map.Entry<byte[], Long> counter : counters.entrySet()) {
      assertTrue(Bytes.equals(counter.getKey(), columns[idx]));
      // evens are +1, odds are +0
      Long expected = idx+1L;
      if (idx % 2 == 0) expected++;
      assertEquals("idx=" + idx, expected, counter.getValue());
      idx++;
    }
  }

  @Test
  public void testSimpleCompareAndSwap() throws OperationException {

    byte [] row = Bytes.toBytes("testSimpleCompareAndSwap");
    byte [] valueOne = Bytes.toBytes("valueOne");
    byte [] valueTwo = Bytes.toBytes("valueTwo");

    this.table.put(row, COL, 1L, valueOne);

    this.table.compareAndSwap(row, COL, valueOne, valueTwo, RP_MAX, 2L);
    try {
      this.table.compareAndSwap(row, COL, valueOne, valueTwo, RP_MAX, 3L);
      fail("Expecting compare-and-swap to fail.");
    } catch (OperationException e) {
      // expected
    }

    assertEquals(Bytes.toString(valueTwo),
        Bytes.toString(this.table.get(row, COL, RP_MAX).getValue()));
    assertEquals(Bytes.toString(valueTwo), Bytes.toString(
        this.table.get(row, COL, new MemoryReadPointer(2L)).getValue()));

    this.table.compareAndSwap(row, COL, valueTwo, valueOne, RP_MAX, 2L);
  }

  @Test
  public void testNullCompareAndSwaps() throws OperationException {

    byte [] row = Bytes.toBytes("testNullCompareAndSwaps");

    byte [] valueOne = Bytes.toBytes("valueOne");
    byte [] valueTwo = Bytes.toBytes("valueTwo");

    assertTrue(this.table.get(row, COL, RP_MAX).isEmpty());

    // compare and swap from null to valueOne
    try {
      this.table.compareAndSwap(row, COL, valueOne, valueTwo, RP_MAX, 2L);
      fail("Expecting compare-and-swap to fail.");
    } catch (OperationException e) {
      // expected
    }
    this.table.compareAndSwap(row, COL, null, valueOne, RP_MAX, 2L);
    this.table.compareAndSwap(row, COL, valueOne, valueTwo, RP_MAX, 3L);

    try {
      this.table.compareAndSwap(row, COL, valueOne, valueTwo, RP_MAX, 4L);
      fail("Expecting compare-and-swap to fail.");
    } catch (OperationException e) {
      // expected
    }

    assertEquals(Bytes.toString(valueTwo),
        Bytes.toString(this.table.get(row, COL, RP_MAX).getValue()));

    try {
      this.table.compareAndSwap(row, COL, null, valueTwo, RP_MAX, 5L);
      fail("Expecting compare-and-swap to fail.");
    } catch (OperationException e) {
      // expected
    }
    this.table.compareAndSwap(row, COL, valueTwo, null, RP_MAX, 5L);

    assertTrue(this.table.get(row, COL, RP_MAX).isEmpty());
  }

  @Test
  public void testCompareAndSwapsWithReadWritePointers()
      throws OperationException {

    byte [] row = Bytes.toBytes("testCompareAndSwapsWithReadWritePointers");

    byte [] valueOne = Bytes.toBytes("valueOne");
    byte [] valueTwo = Bytes.toBytes("valueTwo");

    assertTrue(this.table.get(row, COL, RP_MAX).isEmpty());

    // compare and swap null to valueOne, max to v2
    this.table.compareAndSwap(row, COL, null, valueOne, RP_MAX, 2L);

    // null to valueTwo, read v1 write v3
    this.table.compareAndSwap(
        row, COL, null, valueTwo, new MemoryReadPointer(1L), 3L);

    // read @ 3 gives value two
    assertEquals(Bytes.toString(valueTwo), Bytes.toString(
        this.table.get(row, COL, new MemoryReadPointer(3)).getValue()));

    // read @ 2 gives value one
    assertEquals(Bytes.toString(valueOne), Bytes.toString(
        this.table.get(row, COL, new MemoryReadPointer(2)).getValue()));

    // cas valueOne @ ts2 to valueTwo @ ts4
    this.table.compareAndSwap(
        row, COL, valueOne, valueTwo, new MemoryReadPointer(2L), 4L);

    // cas valueTwo @ ts3 to valueOne @ ts5
    this.table.compareAndSwap(
        row, COL, valueTwo, valueOne, new MemoryReadPointer(3L), 5L);

    // read @ 5 gives value one
    assertEquals(Bytes.toString(valueOne), Bytes.toString(
        this.table.get(row, COL, new MemoryReadPointer(5)).getValue()));

    // read @ 4 gives value two
    assertEquals(Bytes.toString(valueTwo), Bytes.toString(
        this.table.get(row, COL, new MemoryReadPointer(4)).getValue()));

    // cas valueTwo @ ts5 to valueOne @ ts6 FAIL
    try {
      this.table.compareAndSwap(
          row, COL, valueTwo, valueOne, new MemoryReadPointer(5L), 6L);
      fail("Expecting compare-and-swap to fail.");
    } catch (OperationException e) {
      // expected
    }

    // cas valueOne @ ts4 to valueTwo @ ts6 FAIL

    try {
      this.table.compareAndSwap(
          row, COL, valueOne, valueTwo, new MemoryReadPointer(4L), 6L);
      fail("Expecting compare-and-swap to fail.");
    } catch (OperationException e) {
      // expected
    }

    // cas valueOne@5 to valueTwo@5
    assertEquals(Bytes.toString(valueOne), Bytes.toString(
        this.table.get(row, COL, new MemoryReadPointer(5)).getValue()));
    this.table.compareAndSwap(
        row, COL, valueOne, valueTwo, new MemoryReadPointer(5L), 5L);
    assertEquals(Bytes.toString(valueTwo), Bytes.toString(
        this.table.get(row, COL, new MemoryReadPointer(5)).getValue()));

  }

  @Test
  public void testIncrementsSupportReadAndWritePointers()
      throws OperationException {

    byte [] row = Bytes.toBytes("testIncrementsSupportReadAndWritePointers");

    // increment with write pointers

    assertEquals(1L, this.table.increment(row, COL, 1L, RP_MAX, 1L));

    assertEquals(1L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(1L)).getValue()));
    assertEquals(1L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(2L)).getValue()));
    assertEquals(1L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(3L)).getValue()));
    assertEquals(1L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(4L)).getValue()));

    assertEquals(3L, this.table.increment(row, COL, 2L, RP_MAX, 3L));

    assertEquals(1L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(1L)).getValue()));
    assertEquals(1L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(2L)).getValue()));
    assertEquals(3L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(3L)).getValue()));
    assertEquals(3L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(4L)).getValue()));

    // test an increment with a read pointer

    assertEquals(2L, this.table.increment(row, COL, 1L,
        new MemoryReadPointer(1L), 2L));

    // read it back with read pointer reads

    assertEquals(3L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(3L)).getValue()));
    assertEquals(2L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(2L)).getValue()));
    assertEquals(1L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(1L)).getValue()));

    // read it back with increment=0

    assertEquals(1L, this.table.increment(row, COL, 0L,
        new MemoryReadPointer(1L), 1L));
    assertEquals(2L, this.table.increment(row, COL, 0L,
        new MemoryReadPointer(2L), 2L));
    assertEquals(3L, this.table.increment(row, COL, 0L,
        new MemoryReadPointer(3L), 3L));
  }

  @Test
  public void testIncrementCASIncrementWithSameTimestamp()
    throws OperationException {
    byte [] row = Bytes.toBytes("testICASIWSTS");

    // increment with same read and write pointer

    assertEquals(4L, this.table.increment(row, COL, 4L,
                                          new MemoryReadPointer(3L), 3L));
    assertEquals(4L, Bytes.toLong(
      this.table.get(row, COL, new MemoryReadPointer(3L)).getValue()));

    // cas from 4 to 6 @ ts3
    this.table.compareAndSwap(row, COL, Bytes.toBytes(4L),
                              Bytes.toBytes(6L), new MemoryReadPointer(3L), 3L);

    assertEquals(6L, Bytes.toLong(
      this.table.get(row, COL, new MemoryReadPointer(3L)).getValue()));

    // increment to 7 @ ts3
    assertEquals(7L, this.table.increment(row, COL, 1L,
                                          new MemoryReadPointer(3L), 3L));
    assertEquals(7L, Bytes.toLong(
      this.table.get(row, COL, new MemoryReadPointer(3L)).getValue()));

  }

  @Test
  public void testIllegalIncrement()
    throws OperationException {
    byte[] row = Bytes.toBytes("testIlIn");
    byte[] x = {'x'};

    // write a 1-byte value
    this.table.put(row, x, 10L, x);
    try {
      // attempt to increment it
      this.table.increment(row, x, 1L, new MemoryReadPointer(20L), 20L);
      // should throw operation exception ...
      fail("Increment of 1-byte value should have failed");
    } catch (OperationException e) {
      // ... with ILLEGAL_INCREMENT as the status code
      assertEquals(StatusCode.ILLEGAL_INCREMENT, e.getStatus());
    }
  }

  @Test
  public void testSameVersionOverwritesExisting() throws OperationException {

    byte [] row = Bytes.toBytes("testSVOEKey");

    // Write value = 5 @ ts = 5
    this.table.put(row, COL, 5L, Bytes.toBytes(5L));

    // Read value = 5 @ tsMax
    assertEquals(5L,
        Bytes.toLong(this.table.get(row, COL, RP_MAX).getValue()));

    // Write value = 10 @ ts = 10
    this.table.put(row, COL, 10L, Bytes.toBytes(10L));

    // Read value = 10 @ tsMax
    assertEquals(10L,
        Bytes.toLong(this.table.get(row, COL, RP_MAX).getValue()));

    // Write value = 11 @ ts = 10
    this.table.put(row, COL, 10L, Bytes.toBytes(11L));

    // Read value = 11 @ tsMax
    assertEquals(11L,
        Bytes.toLong(this.table.get(row, COL, RP_MAX).getValue()));

    // Read value = 11 @ ts <= 10
    assertEquals(11L, Bytes.toLong(this.table.get(row, COL,
        new MemoryReadPointer(10L)).getValue()));

    // Read value = 5 @ ts <= 9
    assertEquals(5L, Bytes.toLong(this.table.get(row, COL,
        new MemoryReadPointer(9L)).getValue()));

    // Increment + 1 @ ts = 9 to ts=11
    assertEquals(6L, this.table.increment(row, COL, 1L,
        new MemoryReadPointer(9L), 11L));

    // Read value = 6 @ tsMax
    assertEquals(6L, Bytes.toLong(this.table.get(row, COL, RP_MAX).getValue()));

    // CompareAndSwap 6 to 15 @ ts = 11
    this.table.compareAndSwap(row, COL, Bytes.toBytes(6L),
          Bytes.toBytes(15L), new MemoryReadPointer(11L), 12L);

    // Increment + 1 @ ts = 12
    assertEquals(16L, this.table.increment(row, COL, 1L,
        new MemoryReadPointer(12L), 13L));

    // Read value = 16 @ tsMax
    assertEquals(16L,
        Bytes.toLong(this.table.get(row, COL, RP_MAX).getValue()));

    // Read value = 5 @ ts <= 9
    assertEquals(5L, Bytes.toLong(this.table.get(row, COL,
        new MemoryReadPointer(9L)).getValue()));
  }

  @Test
  public void testDeleteBehavior() throws OperationException {

    byte [] row = Bytes.toBytes("testDeleteBehavior");

    // Verify row dne
    assertTrue(this.table.get(row, COL, RP_MAX).isEmpty());

    // Write values 1, 2, 3 @ ts 1, 2, 3
    this.table.put(row, COL, 1L, Bytes.toBytes(1L));
    this.table.put(row, COL, 3L, Bytes.toBytes(3L));
    this.table.put(row, COL, 2L, Bytes.toBytes(2L));

    // Read value, should be 3
    assertEquals(3L, Bytes.toLong(this.table.get(row, COL, RP_MAX).getValue()));

    // Point delete at 2
    this.table.delete(row, COL, 2L);

    // Read value, should be 3
    assertEquals(3L, Bytes.toLong(this.table.get(row, COL, RP_MAX).getValue()));

    // Point delete at 3
    this.table.delete(row, COL, 3L);

    // Read value, should be 1 (2 and 3 point deleted)
    assertEquals(1L, Bytes.toLong(this.table.get(row, COL, RP_MAX).getValue()));

    // DeleteAll at 3
    this.table.deleteAll(row, COL, 3L);

    // Read value, should not exist
    assertTrue(this.table.get(row, COL, RP_MAX).isEmpty());

    // Write at 3 (trying to overwrite existing deletes @ 3)
    this.table.put(row, COL, 3L, Bytes.toBytes(3L));

    // Read value
    // If writes can overwrite deletes at the same timestamp:
    // assertEquals(3L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));
    // Currently, a delete cannot be overwritten on the same version:
    assertTrue(this.table.get(row, COL, RP_MAX).isEmpty());   //fails
    //assertEquals(3L, Bytes.toLong(this.table.get(row, COL, RP_MAX).getValue()));

    // Undelete the delete all at 3
    this.table.undeleteAll(row, COL, 3L);

    // There is still a point delete at 3, should uncover 1
    assertEquals(1L, Bytes.toLong(this.table.get(row, COL, RP_MAX).getValue()));

    // DeleteAll at 5
    this.table.deleteAll(row, COL, 5L);

    // Read value, should not exist
    assertTrue(this.table.get(row, COL, RP_MAX).isEmpty());

    // Write at 4
    this.table.put(row, COL, 4L, Bytes.toBytes(4L));

    // Read value, should not exist
    assertTrue(this.table.get(row, COL, RP_MAX).isEmpty());

    // Write at 6
    this.table.put(row, COL, 6L, Bytes.toBytes(6L));

    // Read value, should be 6
    assertEquals(6L, Bytes.toLong(this.table.get(row, COL, RP_MAX).getValue()));

    // Undelete the delete all at 5
    this.table.undeleteAll(row, COL, 5L);

    // 6 still visible
    assertEquals(6L, Bytes.toLong(this.table.get(row, COL, RP_MAX).getValue()));

    // 4 is still visible at ts=4
    assertEquals(4L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(4)).getValue()));

    // Point delete 6
    this.table.delete(row, COL, 6L);

    // Read value, should now be 4
    assertEquals(4L, Bytes.toLong(this.table.get(row, COL, RP_MAX).getValue()));

  }

  @Test
  public void testGetAllKeys() throws OperationException {

    // list when empty
    List<byte[]> keys = this.table.getKeys(Integer.MAX_VALUE, 0, RP_MAX);
    assertNotNull(keys);
    assertTrue(keys.isEmpty());

    // write 10 rows
    for (int i=0; i<10; i++) {
      this.table.put(Bytes.toBytes("row" + i), COL, 10, Bytes.toBytes(i));
    }

    // get all keys and get all 10 back
    keys = this.table.getKeys(Integer.MAX_VALUE, 0, RP_MAX);
    assertEquals(10, keys.size());
    for (int i=0; i<10; i++) {
      assertTrue("On i=" + i + ", got row " + new String(keys.get(i)),
          Bytes.equals(Bytes.toBytes("row" + i), keys.get(i)));
    }

    // try out a smaller limit
    keys = this.table.getKeys(5, 0, RP_MAX);
    assertEquals(5, keys.size());
    for (int i=0; i<5; i++) {
      assertTrue("On i=" + i + ", got row " + new String(keys.get(i)),
          Bytes.equals(Bytes.toBytes("row" + i), keys.get(i)));
    }

    // try out an offset and limit
    keys = this.table.getKeys(5, 2, RP_MAX);
    assertEquals(5, keys.size());
    for (int i=0; i<5; i++) {
      String row = "row" + (i+2);
      assertTrue("On i=" + i + ", expected row " + row + ", got row " +
          new String(keys.get(i)),
          Bytes.equals(row.getBytes(), keys.get(i)));
    }

    // too big of an offset
    keys = this.table.getKeys(5, 10, RP_MAX);
    assertEquals(0, keys.size());

    // delete three of the rows, undelete one of them
    this.table.delete(Bytes.toBytes("row" + 4), COL, 10);
    this.table.deleteAll(Bytes.toBytes("row" + 6), COL, 10);
    this.table.deleteAll(Bytes.toBytes("row" + 8), COL, 10);
//    this.table.undeleteAll(Bytes.toBytes("row" + 6), COL, 10);
//
//    // get all keys and only get 8 back
//    keys = this.table.getHashKeys(Integer.MAX_VALUE, 0, RP_MAX);
//    assertEquals(8, keys.size());

  }

  class TableCreateThread extends Thread {
    AtomicInteger trigger;
    int stopSignal;
    int lastCreated;
    AtomicBoolean failed;
    int getLastCreated() {
      return this.lastCreated;
    }
    TableCreateThread(AtomicInteger trigger, AtomicBoolean failed,
                      int stopSignal) {
      this.trigger = trigger;
      this.failed = failed;
      this.stopSignal = stopSignal;
      this.lastCreated = trigger.get();
    }
    public void run() {
      while (true) {
        int number = this.trigger.get();
        if (number == stopSignal) break;
        if (number != this.lastCreated) {
          byte[] tableName = Integer.toString(this.trigger.get()).getBytes();
          try {
            getTableHandle().getTable(tableName);
          } catch (Exception e) {
            failed.set(true);
            e.printStackTrace();
            Assert.fail("Creating table '" + this.trigger.get() + "' failed " +
                "with message: " + e.getMessage());
          }
          this.lastCreated = this.trigger.get();
        }
      }
    }
  }

  @Test @Ignore // ignoring this because it runs for minutes on HBase
  public void testConcurrentCreate() throws Exception {
    AtomicInteger trigger = new AtomicInteger(0);
    AtomicBoolean failed = new AtomicBoolean(false);
    TableCreateThread t1 = new TableCreateThread(trigger, failed, -1);
    TableCreateThread t2 = new TableCreateThread(trigger, failed, -1);
    t1.start();
    t2.start();
    System.err.println("both threads started");
    while (trigger.get() < 10) {
      int current = trigger.incrementAndGet();
      System.err.println("triggered " + current);
      while (!failed.get() && (t1.getLastCreated() != current || t2
          .getLastCreated() != current));
      if (failed.get()) break;
      System.err.println("done with " + current);
    }
    trigger.set(-1);
    t1.join();
    t2.join();
    System.err.println("completed." );
    if (failed.get())
      Assert.fail("At least one thread failed");
  }

  @Test
  public void testDeleteDirty() throws OperationException {
    final int MAX = 10;

    // Generate row keys
    byte [] [] rows = new byte[MAX][];
    for(int i = 0; i < MAX; ++i) {
      rows[i] = Bytes.toBytes(this.getClass().getCanonicalName() + ".testDeleteDirty." + i);
      // Verify row[i] dne
      assertTrue(this.table.get(rows[i], RP_MAX).isEmpty());
    }

    for(int i = 0; i < MAX; ++i) {
      // Write values 1, 2, 3 @ ts 1, 2, 3
      this.table.put(rows[i], Bytes.toBytes("col1_" + i), 1L, Bytes.toBytes(1L));
      this.table.put(rows[i], Bytes.toBytes("col2_" + i), 3L, Bytes.toBytes(MAX * 2 + 3L));
      this.table.put(rows[i], Bytes.toBytes("col3_" + i), 2L, Bytes.toBytes(MAX * 3 + 2L));
    }

    for(int i = 0; i < MAX; ++i) {
      // Check written values
      Map<byte[], byte[]> colMap = this.table.get(rows[i], RP_MAX).getValue();
      assertEquals(1, Bytes.toLong(colMap.get(Bytes.toBytes("col1_" + i))));
      assertEquals(MAX * 2 + 3L, Bytes.toLong(colMap.get(Bytes.toBytes("col2_" + i))));
      assertEquals(MAX * 3 + 2L, Bytes.toLong(colMap.get(Bytes.toBytes("col3_" + i))));
    }

    // Delete 3rd, 6th row
    Set<Integer> deletedRows = ImmutableSet.of(3, 6);
    final byte [][] deletedArray = new byte[][] {Bytes.toBytes(3), Bytes.toBytes(6)};
    this.table.deleteDirty(deletedArray);
    // Assert deleted rows are really deleted
    for(Integer i : deletedRows) {
      assertTrue(this.table.get(Bytes.toBytes(i), RP_MAX).isEmpty());
    }
    // Assert other rows still exist
    for(int i = 0; i < MAX; ++i) {
      if(deletedRows.contains(i)) {
        continue;
      }
      Map<byte[], byte[]> colMap = this.table.get(rows[i], RP_MAX).getValue();
      assertEquals(1, Bytes.toLong(colMap.get(Bytes.toBytes("col1_" + i))));
      assertEquals(MAX * 2 + 3L, Bytes.toLong(colMap.get(Bytes.toBytes("col2_" + i))));
      assertEquals(MAX * 3 + 2L, Bytes.toLong(colMap.get(Bytes.toBytes("col3_" + i))));
    }

    // Delete all rows
    for(int i = 0; i < MAX; ++i) {
      this.table.deleteDirty(new byte[][]{rows[i]});
    }

    // Assert all rows are deleted
    for(int i = 0; i < MAX; ++i) {
      assertTrue(this.table.get(rows[i], RP_MAX).isEmpty());
    }
  }

  @Test
  public void testMultiRowPut() throws OperationException {
    final int MAX = 10;

    // Generate row keys, cols, values
    byte [] [] rows = new byte[MAX][];
    byte [] [] cols = new byte[MAX][];
    byte [] [] values = new byte[MAX][];
    for(int i = 0; i < MAX; ++i) {
      rows[i] = Bytes.toBytes(this.getClass().getCanonicalName() + ".testMultiRowPut." + i);
      // Verify row[i] dne
      assertTrue(this.table.get(rows[i], RP_MAX).isEmpty());

      cols[i] = Bytes.toBytes("col" + i);
      values[i] = Bytes.toBytes(i);
    }

    // Put data
    this.table.put(rows, cols, 1L, values);

    // Verify data
    for(int i = 0; i < MAX; ++i) {
      Map<byte[], byte[]> colMap = this.table.get(rows[i], RP_MAX).getValue();
      // Assert only one col per row is read
      assertEquals(1, colMap.size());
      assertEquals(i, Bytes.toInt(colMap.get(cols[i])));
    }
  }

  @Test
  public void testMultiRowMultiColumnPut() throws OperationException {
    final int MAX = 10;

    // Generate row keys, cols, values
    byte [] [] rows = new byte[MAX][];
    byte [] [] [] colsPerRow = new byte[MAX][][];
    byte [] [] [] valuesPerRow = new byte[MAX][][];
    for(int i = 0; i < MAX; ++i) {
      rows[i] = Bytes.toBytes(this.getClass().getCanonicalName() + ".testMultiRowColumnPut." + i);
      // Verify row[i] dne
      assertTrue(this.table.get(rows[i], RP_MAX).isEmpty());
      colsPerRow[i] = new byte[i + 1][];
      valuesPerRow[i] = new byte[i + 1][];
      for (int j = 0; j <= i; ++j) {
        colsPerRow[i][j] = Bytes.toBytes("col" + i + "." + j);
        valuesPerRow[i][j] = Bytes.toBytes(i * j);
      }
    }

    // Put data
    this.table.put(rows, colsPerRow, 1L, valuesPerRow);

    // Verify data
    for(int i = 0; i < MAX; ++i) {
      Map<byte[], byte[]> colMap = this.table.get(rows[i], RP_MAX).getValue();
      // Assert only one col per row is read
      assertEquals(i + 1, colMap.size());
      for (int j = 0; j <= i; j++) {
        assertEquals(i * j, Bytes.toInt(colMap.get(colsPerRow[i][j])));
      }
    }
  }

  @Test
  public void testGetAllColumns() throws OperationException {
    final int MAX = 10;
    final byte[] COL1 = Bytes.toBytes("col1");
    final byte[] COL2 = Bytes.toBytes("col2");

    // Generate row keys, cols, values
    byte [] [] rows = new byte[MAX][];
    byte [] [] cols1 = new byte[MAX][];
    byte [] [] cols2 = new byte[MAX][];
    byte [] [] values1 = new byte[MAX][];
    byte [] [] values2 = new byte[MAX][];
    for(int i = 0; i < MAX; ++i) {
      rows[i] = Bytes.toBytes(this.getClass().getCanonicalName() + ".testGetAllColumns." + i);
      // Verify row[i] dne
      assertTrue(this.table.get(rows[i], RP_MAX).isEmpty());

      cols1[i] = COL1;
      cols2[i] = COL2;
      values1[i] = Bytes.toBytes(i);
      values2[i] = Bytes.toBytes(MAX + i);
    }

    // Put data
    this.table.put(rows, cols1, 1L, values1);
    this.table.put(rows, cols2, 1L, values2);

    // Add some extraneous values
    this.table.put(rows[0], Bytes.toBytes("extra_column"), 1L, Bytes.toBytes(-1));
    this.table.put(rows[1], Bytes.toBytes("extra_column"), 1L, Bytes.toBytes(-1));
    this.table.put(rows[2], Bytes.toBytes("extra_column"), 1L, Bytes.toBytes(-1));


    // Get data
    Map<byte[], Map<byte[], byte[]>> valuesMap = this.table.getAllColumns(rows, new byte[][]{COL1, COL2}, RP_MAX).getValue();
    assertFalse(valuesMap.isEmpty());

    // Verify data
    for(int i = 0; i < MAX; ++i) {
      // Assert two cols per row is read
      assertEquals(2, valuesMap.get(rows[i]).size());
      assertEquals(Bytes.toInt(values1[i]), Bytes.toInt(valuesMap.get(rows[i]).get(cols1[i])));
      assertEquals(Bytes.toInt(values2[i]), Bytes.toInt(valuesMap.get(rows[i]).get(cols2[i])));
    }

    // Get data (single column)
    valuesMap = this.table.getAllColumns(rows, new byte[][]{COL1}, RP_MAX).getValue();
    assertFalse(valuesMap.isEmpty());

    // Verify data
    for(int i = 0; i < MAX; ++i) {
      // Assert one col per row is read
      assertEquals(1, valuesMap.get(rows[i]).size());
      assertEquals(Bytes.toInt(values1[i]), Bytes.toInt(valuesMap.get(rows[i]).get(cols1[i])));
    }
  }


  @Test
  public void testCompareAndSwapDirty() throws OperationException {
    // TODO: need to run multi-threaded to test for atomicity.
    final byte[] ROW = Bytes.toBytes(this.getClass().getCanonicalName() + ".testGetAllColumns");
    final byte[] COL1 = Bytes.toBytes("col1");
    final byte[] COL2 = Bytes.toBytes("col2");

    final byte[] col2Value = Bytes.toBytes("col2Value");
    this.table.put(ROW, COL2, TransactionOracle.DIRTY_WRITE_VERSION, col2Value);

    for(int i = 0; i < 2 ; ++i) {
      String message = "Iteration " + (i+1);
      byte[] expected1 = Bytes.toBytes(1);
      byte[] newValue1 = Bytes.toBytes(10);
      // Cell does not exist yet
      assertFalse(message, this.table.compareAndSwapDirty(ROW, COL1, expected1, newValue1));
      assertTrue(message, this.table.compareAndSwapDirty(ROW, COL1, null, newValue1));

      // Cell has newValue1 now
      byte[] newValue2 = Bytes.toBytes(20);
      assertFalse(message, this.table.compareAndSwapDirty(ROW, COL1, null, newValue2));
      assertTrue(message, this.table.compareAndSwapDirty(ROW, COL1, newValue1, newValue2));

      // Cell has newValue2 now
      byte[] newValue3 = Bytes.toBytes(30);
      assertFalse(message, this.table.compareAndSwapDirty(ROW, COL1, null, newValue3));
      assertFalse(message, this.table.compareAndSwapDirty(ROW, COL1, newValue1, newValue3));
      assertTrue(message, this.table.compareAndSwapDirty(ROW, COL1, newValue2, newValue3));

      // Cell has newValue3 now
      byte[] newValue4 = null;
      assertFalse(message, this.table.compareAndSwapDirty(ROW, COL1, null, newValue4));
      assertFalse(message, this.table.compareAndSwapDirty(ROW, COL1, newValue1, newValue4));
      assertFalse(message, this.table.compareAndSwapDirty(ROW, COL1, newValue2, newValue4));
      assertTrue(message, this.table.compareAndSwapDirty(ROW, COL1, newValue3, newValue4));
    }

    byte[] actualCol2Value = this.table.get(ROW, COL2, TransactionOracle.DIRTY_READ_POINTER).getValue();
    assertEquals(Bytes.toString(col2Value), Bytes.toString(actualCol2Value));
  }

  @Test
  public void testAtomicIncrementDirtily() throws OperationException {
    byte [] row = Bytes.toBytes("testAtomicIncrementDirtily");

    long val = this.table.incrementAtomicDirtily(row,COL,1L);
    assertEquals(1L, val);

    val = this.table.incrementAtomicDirtily(row,COL,1L);
    assertEquals(2L, val);

    val = this.table.incrementAtomicDirtily(row,COL,10L);
    assertEquals(12L, val);

    // -- this will fail right now in some implementations. Will be addressed in ENG-2170
    // this.table.deleteDirty(new byte[][] { row } );
    //
    // val = this.table.incrementAtomicDirtily(row,COL,10L);
    // assertEquals(10L, val);
  }

  @Test
  public void testAtomicIncrementConcurrently() throws OperationException, InterruptedException {
    final byte [] row = Bytes.toBytes("testAtomicIncrementDirtily");
    final int count = 100;
    final OrderedVersionedColumnarTable table = this.table;

    Thread atomicIncrementThread1 = new Thread() {
      @Override
      public void run() {
        for( int i = 0 ; i < count; i++){
          try {
           table.incrementAtomicDirtily(row, COL, 1L);
          } catch (OperationException e) {
            assertTrue(false); //This case should never occur
          }
        }
      }
    };

    Thread atomicIncrementThread2 = new Thread() {
      @Override
      public void run() {
        for( int i = 0 ; i < count; i++){
          try {
            table.incrementAtomicDirtily(row,COL,1L);
          } catch (OperationException e) {
            assertTrue(false); //This case should never occur
          }
        }
      }
    };

    atomicIncrementThread2.start();
    atomicIncrementThread1.start();

    atomicIncrementThread2.join();
    atomicIncrementThread1.join();

    long val  = this.table.incrementAtomicDirtily(row,COL,1L);
    assertEquals(201,val);
  }

  @Test
  public void testIncrementIgnoresInProgressXactions() throws OperationException {
    final byte[] row = "tIIIPX".getBytes();
    final byte[] c1 = { 'c', '1' };
    final byte[] one = com.continuuity.api.common.Bytes.toBytes(1L);

    // execute a write in a new xaction
    Transaction tx = new Transaction(1, new MemoryReadPointer(1, 1, new HashSet<Long>()), true);
    this.table.put(row, c1, tx.getWriteVersion(), one);
    // read from outside xaction -> not visible
    ReadPointer outside = new MemoryReadPointer(2, 2, Collections.singleton(1L));
    OperationResult<byte[]> result = this.table.get(row, c1, outside);
    assertTrue(result.isEmpty() || result.getValue() == null);
    // increment outside xaction -> increments pre-xaction value
    Map<byte[], Long> value = this.table.increment(row, new byte[][]{ c1 }, new long[]{ 4L }, outside,
                                                   outside.getMaximum());
    assertEquals(new Long(4L), value.get(c1));
  }



  @Test
  public void testScan() throws OperationException {
    final byte [] row = "scanTests".getBytes(Charsets.UTF_8);
    final byte [] col = "c".getBytes(Charsets.UTF_8);

    Transaction tx = new Transaction(1, new MemoryReadPointer(1, 1, new HashSet<Long>()), true);

    for (int i = 100; i < 200; i++) {
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.put(rowKey, col, tx.getWriteVersion(), Bytes.toBytes(i));
    }

    byte [] startRow = Bytes.add(row, Bytes.toBytes(100));
    byte [] stopRow = Bytes.add(row, Bytes.toBytes(150));

    Scanner scanner = this.table.scan(startRow, stopRow, TransactionOracle.DIRTY_READ_POINTER);
    assertTrue(scanner != null);

    int firstEntry = Bytes.toInt(scanner.next().getSecond().get(col));
    assertEquals(100, firstEntry);

    int count = 1; //count is set to one since we already read one entry
    int lastEntry = -1;

    boolean done = false;
    while (!done) {
      ImmutablePair<byte[], Map<byte[], byte[]>> r = scanner.next();
      if (r == null) {
        done = true;
      } else {
        lastEntry = Bytes.toInt(r.getSecond().get(col));
        count++;
     }
    }

    assertEquals(50, count); // checks if we got required amount of entry
    assertEquals(149, lastEntry); // checks if the scan interval [startRow, stopRow) works fine
  }

  @Test
  public void testScanMid() throws OperationException {
    final byte [] row = "scanTests".getBytes(Charsets.UTF_8);
    final byte [] col = "c".getBytes(Charsets.UTF_8);

    Transaction tx = new Transaction(1, new MemoryReadPointer(1, 1, new HashSet<Long>()), true);

    for (int i = 100; i < 200; i++) {
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.put(rowKey, col, tx.getWriteVersion(), Bytes.toBytes(i));
    }

    byte [] startRow = Bytes.add(row, Bytes.toBytes(150));
    byte [] stopRow = Bytes.add(row, Bytes.toBytes(175));

    Scanner scanner = this.table.scan(startRow, stopRow, TransactionOracle.DIRTY_READ_POINTER);

    assertTrue(scanner != null);

    int firstEntry = Bytes.toInt(scanner.next().getSecond().get(col));
    assertEquals(150, firstEntry);

    int count = 1; //count is set to one since we already read one entry
    int lastEntry = -1;

    boolean done = false;
    while (!done) {
      ImmutablePair<byte[], Map<byte[], byte[]>> r = scanner.next();
      if (r == null) {
        done = true;
      } else {
        lastEntry = Bytes.toInt(r.getSecond().get(col));
        count++;
      }
    }

    assertEquals(25, count); // checks if we got required amount of entry
    assertEquals(174, lastEntry); // checks if the scan interval [startRow, stopRow) works fine
  }

  @Test
  public void testScanNoResult() throws OperationException {
    final byte [] row = "scanTests".getBytes(Charsets.UTF_8);
    final byte [] col = "c".getBytes(Charsets.UTF_8);

    Transaction tx = new Transaction(1, new MemoryReadPointer(1, 1, new HashSet<Long>()), true);

    for (int i = 100; i < 200; i++) {
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.put(rowKey, col, tx.getWriteVersion(), Bytes.toBytes(i));
    }

    byte [] startRow = Bytes.add(row, Bytes.toBytes(210));
    byte [] stopRow = Bytes.add(row, Bytes.toBytes(250));

    Scanner scanner = this.table.scan(startRow, stopRow, TransactionOracle.DIRTY_READ_POINTER);
    assertTrue(scanner != null);

    int count = 0;
    while (scanner.next() != null) {
      count++;
    }

    assertEquals(0, count);
  }

  @Test
  public void testScanWithDeletes() throws OperationException {
    final byte [] row = "scanTestsWithDeletes".getBytes(Charsets.UTF_8);
    final byte [] col = "c".getBytes(Charsets.UTF_8);

    Transaction tx = new Transaction(1, new MemoryReadPointer(1, 1, new HashSet<Long>()), true);

    for (int i = 100; i < 200; i++) {
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.put(rowKey, col, tx.getWriteVersion(), Bytes.toBytes(i));
    }

    byte [] deleteKey1 = Bytes.add(row, Bytes.toBytes(101));
    byte [] deleteKey2 = Bytes.add(row, Bytes.toBytes(130));
    byte [] deleteKey3 = Bytes.add(row, Bytes.toBytes(148));
    byte [] deleteKey4 = Bytes.add(row, Bytes.toBytes(150));


    this.table.delete(deleteKey1, col, tx.getWriteVersion());
    this.table.delete(deleteKey2, col, tx.getWriteVersion());
    this.table.delete(deleteKey3, col, tx.getWriteVersion());
    this.table.delete(deleteKey4, col, tx.getWriteVersion());


    byte [] startRow = Bytes.add(row, Bytes.toBytes(100));
    byte [] stopRow = Bytes.add(row, Bytes.toBytes(150));

    Scanner scanner = this.table.scan(startRow, stopRow, TransactionOracle.DIRTY_READ_POINTER);
    assertTrue(scanner != null);

    int firstEntry = Bytes.toInt(scanner.next().getSecond().get(col));
    assertEquals(100, firstEntry);

    int count = 1; //count is set to one since we already read one entry
    int lastEntry = -1;

    boolean done = false;
    while (!done) {
      ImmutablePair<byte[], Map<byte[], byte[]>> r = scanner.next();
      if (r == null) {
        done = true;
      } else {

        lastEntry = Bytes.toInt(r.getSecond().get(col));
        count++;
      }
    }

    assertEquals(47, count);
    assertEquals(149, lastEntry);
  }

  @Test
  public void testScansWithMultipleColumns() throws OperationException{
    final byte [] row = "scanTestsWithTwoColumns".getBytes(Charsets.UTF_8);
    final byte [] col1 = "c1".getBytes(Charsets.UTF_8);
    final byte [] col2 = "c2".getBytes(Charsets.UTF_8);

    Transaction tx = new Transaction(1, new MemoryReadPointer(1, 1, new HashSet<Long>()), true);

    for (int i = 100; i < 200; i++) {
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.put(rowKey, col1, tx.getWriteVersion(), Bytes.toBytes(i));
      this.table.put(rowKey, col2, tx.getWriteVersion(), Bytes.toBytes(2 * i));
    }

    byte [] startRow = Bytes.add(row, Bytes.toBytes(100));
    byte [] stopRow = Bytes.add(row, Bytes.toBytes(150));

    OperationResult<Map<byte[], byte[]>> result = this.table.get(startRow, tx.getReadPointer());

    assertEquals(100, Bytes.toInt(result.getValue().get(col1)));
    assertEquals(200, Bytes.toInt(result.getValue().get(col2)));

    Scanner scanner = this.table.scan(startRow, stopRow, TransactionOracle.DIRTY_READ_POINTER);
    assertTrue(scanner != null);

    ImmutablePair<byte[], Map<byte[], byte[]>> firstResult = scanner.next();

    int firstColEntry = Bytes.toInt(firstResult.getSecond().get(col1));
    assertEquals(100, firstColEntry);

    int secondColEntry = Bytes.toInt(firstResult.getSecond().get(col2));
    assertEquals(200, secondColEntry);

    int count = 1;  //count is set to one since we already read one entry
    int lastEntryCol1 = -1;
    int lastEntryCol2 = -1;

    boolean done = false;
    while (!done) {
      ImmutablePair<byte[], Map<byte[], byte[]>> r = scanner.next();
      if (r == null) {
        done = true;
      } else {
        lastEntryCol1 = Bytes.toInt(r.getSecond().get(col1));
        lastEntryCol2 = Bytes.toInt(r.getSecond().get(col2));
        assertEquals(2 * lastEntryCol1, lastEntryCol2);
        count++;
      }
    }

    assertEquals(50, count);
    assertEquals(149, lastEntryCol1);
    assertEquals(2 * 149, lastEntryCol2);
  }

  @Test
  public void testScanStartRowNull() throws OperationException {
    final byte [] row = "scanTestsStartRowNull".getBytes(Charsets.UTF_8);
    final byte [] col = "c".getBytes(Charsets.UTF_8);

    Transaction tx = new Transaction(1, new MemoryReadPointer(1, 1, new HashSet<Long>()), true);

    for (int i = 100; i < 200; i++) {
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.put(rowKey, col, tx.getWriteVersion(), Bytes.toBytes(i));
    }

    byte [] startRow = null;
    byte [] stopRow = Bytes.add(row, Bytes.toBytes(175));

    Scanner scanner = this.table.scan(startRow, stopRow, TransactionOracle.DIRTY_READ_POINTER);

    assertTrue(scanner != null);

    int firstEntry = Bytes.toInt(scanner.next().getSecond().get(col));
    assertEquals(100, firstEntry);

    int count = 1; //count is set to one since we already read one entry
    int lastEntry = -1;

    boolean done = false;
    while (!done) {
      ImmutablePair<byte[], Map<byte[], byte[]>> r = scanner.next();
      if (r == null) {
        done = true;
      } else {
        lastEntry = Bytes.toInt(r.getSecond().get(col));
        count++;
      }
    }

    assertEquals(75, count); // checks if we got required amount of entry
    assertEquals(174, lastEntry); // checks if the scan interval [startRow, stopRow) works fine
  }

  @Test
  public void testScanStopRowNull() throws OperationException {
    final byte [] row = "scanTestsStopRowNull".getBytes(Charsets.UTF_8);
    final byte [] col = "c".getBytes(Charsets.UTF_8);

    Transaction tx = new Transaction(1, new MemoryReadPointer(1, 1, new HashSet<Long>()), true);

    for (int i = 100; i < 200; i++) {
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.put(rowKey, col, tx.getWriteVersion(), Bytes.toBytes(i));
    }

    byte [] startRow = Bytes.add(row, Bytes.toBytes(150));
    byte [] stopRow = null;

    Scanner scanner = this.table.scan(startRow, stopRow, TransactionOracle.DIRTY_READ_POINTER);

    assertTrue(scanner != null);

    int firstEntry = Bytes.toInt(scanner.next().getSecond().get(col));
    assertEquals(150, firstEntry);

    int count = 1; //count is set to one since we already read one entry
    int lastEntry = -1;

    boolean done = false;
    while (!done) {
      ImmutablePair<byte[], Map<byte[], byte[]>> r = scanner.next();
      if (r == null) {
        done = true;
      } else {
        lastEntry = Bytes.toInt(r.getSecond().get(col));
        count++;
      }
    }

    assertEquals(50, count); // checks if we got required amount of entry
    assertEquals(199, lastEntry); // checks if the scan interval [startRow, stopRow) works fine
  }


  @Test
  public void testScanStartAndStopRowNull() throws OperationException {
    final byte [] row = "scanTestsStartStopRowNull".getBytes(Charsets.UTF_8);
    final byte [] col = "c".getBytes(Charsets.UTF_8);

    Transaction tx = new Transaction(1, new MemoryReadPointer(1, 1, new HashSet<Long>()), true);

    for (int i = 100; i < 200; i++) {
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.put(rowKey, col, tx.getWriteVersion(), Bytes.toBytes(i));
    }

    byte [] startRow = null;
    byte [] stopRow = null;

    Scanner scanner = this.table.scan(startRow, stopRow, TransactionOracle.DIRTY_READ_POINTER);

    assertTrue(scanner != null);

    int firstEntry = Bytes.toInt(scanner.next().getSecond().get(col));
    assertEquals(100, firstEntry);

    int count = 1; //count is set to one since we already read one entry
    int lastEntry = -1;

    boolean done = false;
    while (!done) {
      ImmutablePair<byte[], Map<byte[], byte[]>> r = scanner.next();
      if (r == null) {
        done = true;
      } else {
        lastEntry = Bytes.toInt(r.getSecond().get(col));
        count++;
      }
    }

    assertEquals(100, count); // checks if we got required amount of entry
    assertEquals(199, lastEntry); // checks if the scan interval [startRow, stopRow) works fine
  }

  /**
   * Write two versions of the same column - Delete the higher version and perform scan.
   * The results should return lower versions for all columns.
   * @throws OperationException
   */
  @Test
  public void testScanWithDeleteHigherVersion() throws OperationException {
    final byte [] row = "scanTestsWithDeleteSingleColumn".getBytes(Charsets.UTF_8);
    final byte [] col1 = "c1".getBytes(Charsets.UTF_8);
    final byte [] col2 = "c2".getBytes(Charsets.UTF_8);
    final byte [] col3 = "c3".getBytes(Charsets.UTF_8);

    Transaction tx = new Transaction(1, new MemoryReadPointer(Long.MAX_VALUE, Long.MAX_VALUE, new HashSet<Long>()), true);
    long version = 1L;

    for (int i = 100; i < 200; i++) {
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.put(rowKey, col1, version, Bytes.toBytes(i));
      this.table.put(rowKey, col2, version, Bytes.toBytes(2 * i));
      this.table.put(rowKey, col3, version, Bytes.toBytes(3 * i));
    }

    long highVersion = 2L;
    for (int i = 100; i < 200; i++) {
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.put(rowKey, col1, highVersion, Bytes.toBytes(0));
      this.table.put(rowKey, col2, highVersion, Bytes.toBytes(0));
      this.table.put(rowKey, col3, highVersion, Bytes.toBytes(0));
    }

    for (int i = 100; i < 200; i++){
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.delete(rowKey, col1, highVersion);
    }

    byte [] startRow = Bytes.add(row, Bytes.toBytes(100));
    byte [] stopRow = Bytes.add(row, Bytes.toBytes(150));

    Scanner scanner = this.table.scan(startRow, stopRow, tx);
    assertTrue(scanner != null);

    ImmutablePair<byte[], Map<byte[], byte[]>> entry = scanner.next();

    int firstEntryCol1 = Bytes.toInt(entry.getSecond().get(col1));
    assertEquals(100, firstEntryCol1);

    int firstEntryCol2 = Bytes.toInt(entry.getSecond().get(col2));
    assertEquals(0, firstEntryCol2);
  }

  /**
   * Insert into 3 different columns for each row. Perform deleteAll on column1. Check if
   * scanner returns other two columns.
   * @throws OperationException
   */
  @Test
  public void testScanWithDeleteAllHigherVersion() throws OperationException {
    final byte [] row = "scanTestsWithDeleteSingleColumn".getBytes(Charsets.UTF_8);
    final byte [] col1 = "c1".getBytes(Charsets.UTF_8);
    final byte [] col2 = "c2".getBytes(Charsets.UTF_8);
    final byte [] col3 = "c3".getBytes(Charsets.UTF_8);

    Transaction tx = new Transaction(1, new MemoryReadPointer(Long.MAX_VALUE, Long.MAX_VALUE, new HashSet<Long>()), true);
    long version = 1L;

    for (int i = 100; i < 200; i++) {
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.put(rowKey, col1, version, Bytes.toBytes(i));
      this.table.put(rowKey, col2, version, Bytes.toBytes(2 * i));
      this.table.put(rowKey, col3, version, Bytes.toBytes(3 * i));
    }

    long highVersion = 2L;
    for (int i = 100; i < 200; i++) {
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.put(rowKey, col1, highVersion, Bytes.toBytes(0));
      this.table.put(rowKey, col2, highVersion, Bytes.toBytes(1));
      this.table.put(rowKey, col3, highVersion, Bytes.toBytes(2));
    }

    for (int i = 100; i < 200; i++){
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.deleteAll(rowKey, col1, highVersion);
    }

    byte [] startRow = Bytes.add(row, Bytes.toBytes(100));
    byte [] stopRow = Bytes.add(row, Bytes.toBytes(150));

    Scanner scanner = this.table.scan(startRow, stopRow, tx);
    assertTrue(scanner != null);

    ImmutablePair<byte[], Map<byte[], byte[]>> entry = scanner.next();


    assertTrue(null == entry.getSecond().get(col1));

    int firstEntryCol2 = Bytes.toInt(entry.getSecond().get(col2));
    assertEquals(1, firstEntryCol2);

    int firstEntryCol3 = Bytes.toInt(entry.getSecond().get(col3));
    assertEquals(2, firstEntryCol3);

    int count = 2;

    boolean done = false;
    while (!done) {
      ImmutablePair<byte[], Map<byte[], byte[]>> r = scanner.next();
      if (r == null) {
        done = true;
      } else {
        assertTrue(null == r.getSecond().get(col1));
        assertEquals(1, Bytes.toInt(r.getSecond().get(col2)));
        assertEquals(2, Bytes.toInt(r.getSecond().get(col3)));
        count += 2;
      }
    }
    assertEquals(100, count); //50 rows and 2 cols
  }

  /**
   * Insert into three cols at version v1, Insert into three columns with version v2.
   * Delete lower version. Scan should return all higher versioned columns.
   * @throws OperationException
   */
  @Test
  public void testScanWithDeleteAllLowerVersion() throws OperationException {
    final byte [] row = "scanTestsWithDeleteSingleColumn".getBytes(Charsets.UTF_8);
    final byte [] col1 = "c1".getBytes(Charsets.UTF_8);
    final byte [] col2 = "c2".getBytes(Charsets.UTF_8);
    final byte [] col3 = "c3".getBytes(Charsets.UTF_8);

    Transaction tx = new Transaction(1, new MemoryReadPointer(Long.MAX_VALUE, Long.MAX_VALUE, new HashSet<Long>()), true);
    long lowVersion = 1L;

    for (int i = 100; i < 200; i++) {
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.put(rowKey, col1, lowVersion, Bytes.toBytes(i));
      this.table.put(rowKey, col2, lowVersion, Bytes.toBytes(2 * i));
      this.table.put(rowKey, col3, lowVersion, Bytes.toBytes(3 * i));
    }

    long highVersion = 2L;
    for (int i = 100; i < 200; i++) {
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.put(rowKey, col1, highVersion, Bytes.toBytes(0));
      this.table.put(rowKey, col2, highVersion, Bytes.toBytes(1));
      this.table.put(rowKey, col3, highVersion, Bytes.toBytes(2));
    }

    for (int i = 100; i < 200; i++){
      byte [] rowKey = Bytes.add(row, Bytes.toBytes(i));
      this.table.deleteAll(rowKey, col1, lowVersion);
    }

    byte [] startRow = Bytes.add(row, Bytes.toBytes(100));
    byte [] stopRow = Bytes.add(row, Bytes.toBytes(150));

    Scanner scanner = this.table.scan(startRow, stopRow, tx);
    assertTrue(scanner != null);

    ImmutablePair<byte[], Map<byte[], byte[]>> entry = scanner.next();

    int firstEntryCol1 = Bytes.toInt(entry.getSecond().get(col1));
    assertEquals(0, firstEntryCol1);

    int firstEntryCol2 = Bytes.toInt(entry.getSecond().get(col2));
    assertEquals(1, firstEntryCol2);

    int firstEntryCol3 = Bytes.toInt(entry.getSecond().get(col3));
    assertEquals(2, firstEntryCol3);

    int count = 3;

    boolean done = false;
    while (!done) {
      ImmutablePair<byte[], Map<byte[], byte[]>> r = scanner.next();
      if (r == null) {
        done = true;
      } else {
        assertEquals(0, Bytes.toInt(r.getSecond().get(col1)));
        assertEquals(1, Bytes.toInt(r.getSecond().get(col2)));
        assertEquals(2, Bytes.toInt(r.getSecond().get(col3)));
        count += 3;
      }
    }
    assertEquals(150, count); //50 rows and 3 cols
  }

  /**
   * Write ( col1, v1), (col2, v1), (col2, v2), (col3, v2).
   * Delete (col2, v2), (col3, v2)
   * DeleteAll (col1, v2)
   * Scan all records -> should return (col2, v1) and nothing else
   *
   * @throws OperationException
   */
  @Test
  public void testScanDeletesWithMultipleVersions() throws OperationException {
    final byte [] rowKeyPrefix = "scanTestsDeletesWithVersions".getBytes(Charsets.UTF_8);
    final byte [] col1 = "c1".getBytes(Charsets.UTF_8);
    final byte [] col2 = "c2".getBytes(Charsets.UTF_8);
    final byte [] col3 = "c3".getBytes(Charsets.UTF_8);

    byte [] rowKey = Bytes.add(rowKeyPrefix, Bytes.toBytes(10));
    byte [] value = Bytes.toBytes(1);
    byte [] highValue = Bytes.toBytes(2);

    long version = 1L;
    this.table.put(rowKey, col1, version, value);
    this.table.put(rowKey, col2, version, value);

    long highVersion = 2L;
    this.table.put(rowKey, col2, highVersion, highValue);
    this.table.put(rowKey, col3, highVersion, highValue);

    this.table.delete(rowKey, col2, highVersion);
    this.table.delete(rowKey, col3, highVersion);
    this.table.deleteAll(rowKey, col1, highVersion);

    Transaction tx = new Transaction(1, new MemoryReadPointer(Long.MAX_VALUE, Long.MAX_VALUE, new HashSet<Long>()), true);

    Scanner scanner = this.table.scan(tx);

    assertTrue(scanner != null);
    ImmutablePair<byte[], Map<byte[], byte[]>> entry = scanner.next();

    assertTrue(Bytes.equals(entry.getSecond().get(col2), value));
    assertTrue(entry.getSecond().get(col1) == null);
    assertTrue(entry.getSecond().get(col3) == null);

    assertTrue(null == scanner.next());
  }

  /**
   * Write (col1, v1), (col1, v2), (col2, v1), (col2, v2)
   * Delete (col1, v2), DeleteAll(col2, v2)
   * Scan -> Should return (col1, v1) and (col2, v2)
   * @throws OperationException
   */
  @Test @Ignore
  //Ignoring this test case - since the undeleteAll behavior is not consistent across all implementations
  //This test fails for HbaseNative and Hbase implementations and passes on other implementations.
  //Created a jira to address this -  ENG-2438
  public void testScansWithUndelete() throws OperationException {
    final byte [] rowKeyPrefix = "scanTestsUndeletes".getBytes(Charsets.UTF_8);
    final byte [] col1 = "c1".getBytes(Charsets.UTF_8);
    final byte [] col2 = "c2".getBytes(Charsets.UTF_8);

    byte [] rowKey = Bytes.add(rowKeyPrefix, Bytes.toBytes(10));
    byte [] value = Bytes.toBytes(1);
    byte [] highValue = Bytes.toBytes(2);

    long version = 1L;
    this.table.put(rowKey, col1, version, value);
    this.table.put(rowKey, col2, version, value);

    long highVersion = 2L;
    this.table.put(rowKey, col1, highVersion, highValue);
    this.table.put(rowKey, col2, highVersion, highValue);

    this.table.delete(rowKey, col1, highVersion);
    this.table.deleteAll(rowKey, col2, highVersion);

    this.table.undeleteAll(rowKey, col1, highVersion);
    this.table.undeleteAll(rowKey, col2, highVersion);

    Transaction tx = new Transaction(1, new MemoryReadPointer(Long.MAX_VALUE, Long.MAX_VALUE, new HashSet<Long>()), true);

    Scanner scanner = this.table.scan(tx);

    assertTrue(scanner != null);
    ImmutablePair<byte[], Map<byte[], byte[]>> entry = scanner.next();

    assertTrue(Bytes.equals(entry.getSecond().get(col1), value));
    assertTrue(Bytes.equals(entry.getSecond().get(col2), highValue));
  }

  /**
   * Write (col1, v1), (col1, v2), (col2, v1), (col2, v2)
   * Delete (col1, v2), DeleteAll(col2, v2)
   * Get (row, [col1, col2]) -> Should return (col1, v1) and (col2, v2)
   * @throws OperationException
   */
  @Test @Ignore
  //Ignoring this test case - since the undeleteAll behavior is not consistent across all implementations
  //This test fails for HbaseNative and Hbase implementations and passes on other implementations.
  //Created a jira to address this -  ENG-2438
  public void testUndeleteAndGet() throws OperationException{
    final byte [] col1 = "c1".getBytes(Charsets.UTF_8);
    final byte [] col2 = "c2".getBytes(Charsets.UTF_8);

    byte [] rowKey = Bytes.add(Bytes.toBytes("getsWithUndelete"), Bytes.toBytes(10));
    byte [] value = Bytes.toBytes(1);
    byte [] highValue = Bytes.toBytes(2);

    long version = 1L;
    this.table.put(rowKey, col1, version, value);
    this.table.put(rowKey, col2, version, value);

    long highVersion = 2L;
    this.table.put(rowKey, col1, highVersion, highValue);
    this.table.put(rowKey, col2, highVersion, highValue);

    this.table.delete(rowKey, col1, highVersion);
    this.table.deleteAll(rowKey, col2, highVersion);

    this.table.undeleteAll(rowKey, col1, highVersion);
    this.table.undeleteAll(rowKey, col2, highVersion);

    Transaction tx = new Transaction(1, new MemoryReadPointer(Long.MAX_VALUE, Long.MAX_VALUE, new HashSet<Long>()), true);

    OperationResult<byte[]> entryCol1 = this.table.get(rowKey, col1,  tx);
    OperationResult<byte[]> entryCol2 = this.table.get(rowKey, col2,  tx);

    assertTrue(Bytes.equals(entryCol1.getValue(), value));
    assertTrue(Bytes.equals(entryCol2.getValue(), highValue));

  }

}
