package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Tests the contract and semantics of {@link OrderedVersionedColumnarTable}
 * against each of the implementations.
 */
public abstract class TestOVCTable {

  // TODO: As part of ENG-211, add testing of HBaseNativeOVCTable
  private static final Logger Log = LoggerFactory.getLogger(TestOVCTable.class);

  protected OrderedVersionedColumnarTable table;
  private OVCTableHandle tableHandle;

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
    colMap = this.table.get(row, new MemoryReadPointer(9)).getValue();
    assertEquals(0, colMap.size());

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
//    keys = this.table.getKeys(Integer.MAX_VALUE, 0, RP_MAX);
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
}
