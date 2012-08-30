package com.continuuity.data.table;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * Tests the contract and semantics of {@link OrderedVersionedColumnarTable}
 * against each of the implementations.
 */
public abstract class TestOVCTable {

  // TODO: As part of ENG-211, add testing of HBaseOVCTable

  private OVCTableHandle tableHandle;
  private OrderedVersionedColumnarTable table;

  private static final Random r = new Random();

  @Before
  public void initialize() {
    System.out.println("\n\nBeginning test\n\n");
    this.tableHandle = getTableHandle();
    this.table = this.tableHandle.getTable(
        Bytes.toBytes("TestOVCTable" + Math.abs(r.nextInt())));
  }

  protected abstract OVCTableHandle getTableHandle();

  private static final byte [] COL = new byte [] { (byte)0 };
  private static final MemoryReadPointer RP_MAX =
      new MemoryReadPointer(Long.MAX_VALUE);

  @Test
  public void testSimpleReadWrite() {

    byte [] row = Bytes.toBytes("testSimpleReadWrite");

    this.table.put(row, COL, 1L, row);

    assertEquals(Bytes.toString(row),
        Bytes.toString(this.table.get(row, COL, RP_MAX)));
    assertEquals(Bytes.toString(row),
        Bytes.toString(this.table.get(row, COL, new MemoryReadPointer(1L))));

  }

  @Test
  public void testClearVerySimply() {
    byte [] row = Bytes.toBytes("testClear");

    assertNull(this.table.get(row, COL, RP_MAX));

    this.table.put(row, COL, 1L, row);

    assertEquals(Bytes.toString(row),
        Bytes.toString(this.table.get(row, COL, RP_MAX)));

    this.table.clear();

    assertNull(this.table.get(row, COL, RP_MAX));

    this.table.put(row, COL, 1L, row);

    assertEquals(Bytes.toString(row),
        Bytes.toString(this.table.get(row, COL, RP_MAX)));

  }

  @Test
  public void testMultiColumnReadsAndWrites() {

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
      this.table.put(row, new byte [][] { columns[i], columns[i+1] }, version,
          new byte [][] { values[i], values[i+1] });
    }

    // read them all back at once using all the various read apis

    // get(row)
    Map<byte[],byte[]> colMap = this.table.get(row, RP_MAX);
    assertEquals(ncols, colMap.size());
    int idx=0;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,col)
    for(int i=0;i<ncols;i++) {
      byte [] value = this.table.get(row, columns[i], RP_MAX);
      assertTrue(Bytes.equals(value, values[i]));
    }

    // getWV(row,col)
    for(int i=0;i<ncols;i++) {
      ImmutablePair<byte[],Long> valueAndVersion =
          this.table.getWithVersion(row, columns[i], RP_MAX);
      assertTrue(Bytes.equals(valueAndVersion.getFirst(), values[i]));
      assertEquals(new Long(version), valueAndVersion.getSecond());
    }

    // get(row,startCol,stopCol)

    // get(row,start=null,stop=null)
    colMap = this.table.get(row, null, null, RP_MAX);
    assertEquals(ncols, colMap.size());
    idx=0;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,start=0,stop=ncols+1)
    colMap = this.table.get(row, Bytes.toBytes((long)0),
        Bytes.toBytes((long)ncols+1), RP_MAX);
    assertEquals(ncols, colMap.size());
    idx=0;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,cols[ncols])
    colMap = this.table.get(row, columns, RP_MAX);
    assertEquals(ncols, colMap.size());
    idx=0;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,cols[ncols-2])
    byte [][] subCols = (byte[][])Arrays.copyOfRange(columns, 1, ncols - 1);
    colMap = this.table.get(row, subCols, RP_MAX);
    assertEquals(ncols - 2, colMap.size());
    idx=1;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,RP=9) = 0 cols
    colMap = this.table.get(row, new MemoryReadPointer(9));
    assertEquals(0, colMap.size());

    // delete the first 5 as point deletes
    subCols = Arrays.copyOfRange(columns, 0, 5);
    this.table.delete(row, subCols, version);

    // get returns 5 less
    colMap = this.table.get(row, RP_MAX);
    assertEquals(ncols - 5, colMap.size());

    // delete the second 5 as delete alls
    subCols = Arrays.copyOfRange(columns, 5, 10);
    this.table.deleteAll(row, subCols, version);

    // get returns 10 less
    colMap = this.table.get(row, RP_MAX);
    assertEquals(ncols - 10, colMap.size());

    // delete the third 5 as delete alls
    subCols = Arrays.copyOfRange(columns, 10, 15);
    this.table.deleteAll(row, subCols, version);

    // get returns 15 less
    colMap = this.table.get(row, RP_MAX);
    assertEquals(ncols - 15, colMap.size());

    // undelete the second 5
    subCols = Arrays.copyOfRange(columns, 5, 10);
    this.table.undeleteAll(row, subCols, version);

    // get returns 10 less
    colMap = this.table.get(row, RP_MAX);
    assertEquals(ncols - 10, colMap.size());
  }

  @Test
  public void testReadColumnRange() {

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
      this.table.put(row, new byte [][] { columns[i], columns[i+1] }, version,
          new byte [][] { values[i], values[i+1] });
    }

    // get(row,startCol,stopCol)
    // get(row,start=null,stop=null)
    Map<byte[],byte[]> colMap = this.table.get(row, null, null, RP_MAX);
    assertEquals(ncols, colMap.size());
    int idx=0;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,start=0,stop=ncols+1)
    colMap = this.table.get(row, Bytes.toBytes((long)0),
        Bytes.toBytes((long)ncols+1), RP_MAX);
    assertEquals(ncols, colMap.size());
    idx=0;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,start=1,stop=ncols-1) = ncols-2
    colMap = this.table.get(row, Bytes.toBytes((long)1),
        Bytes.toBytes((long)ncols-1), RP_MAX);
    assertEquals(ncols - 2, colMap.size());
    idx=1;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

    // get(row,start=10,stop=20) = 10
    colMap = this.table.get(row, Bytes.toBytes((long)10),
        Bytes.toBytes((long)20), RP_MAX);
    assertEquals(10, colMap.size());
    idx=10;
    for (Map.Entry<byte[], byte[]> entry : colMap.entrySet()) {
      assertTrue(Bytes.equals(entry.getKey(), columns[idx]));
      assertTrue(Bytes.equals(entry.getValue(), values[idx]));
      idx++;
    }

  }

  @Test
  public void testSimpleIncrement() {

    byte [] row = Bytes.toBytes("testSimpleIncrement");

    assertEquals(1L, this.table.increment(row, COL, 1L, RP_MAX, 1L));

    assertEquals(3L, this.table.increment(row, COL, 2L, RP_MAX, 2L));

    assertEquals(3L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));

  }

  @Test
  public void testMultiColumnIncrement() {

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
      Long expected = new Long(idx+1);
      if (idx % 2 == 0) expected++;
      assertEquals("idx=" + idx, expected, counter.getValue());
      idx++;
    }
  }

  @Test
  public void testSimpleCompareAndSwap() {

    byte [] row = Bytes.toBytes("testSimpleCompareAndSwap");

    byte [] valueOne = Bytes.toBytes("valueOne");
    byte [] valueTwo = Bytes.toBytes("valueTwo");

    this.table.put(row, COL, 1L, valueOne);

    try {
      assertTrue(
          this.table.compareAndSwap(row, COL, valueOne, valueTwo, RP_MAX, 2L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    try {
      assertFalse(
          this.table.compareAndSwap(row, COL, valueOne, valueTwo, RP_MAX, 3L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    assertEquals(Bytes.toString(valueTwo),
        Bytes.toString(this.table.get(row, COL, RP_MAX)));
    assertEquals(Bytes.toString(valueTwo),
        Bytes.toString(this.table.get(row, COL, new MemoryReadPointer(2L))));

    try {
      assertTrue(
          this.table.compareAndSwap(row, COL, valueTwo, valueOne, RP_MAX, 2L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }

  @Test
  public void testNullCompareAndSwaps() {

    byte [] row = Bytes.toBytes("testNullCompareAndSwaps");

    byte [] valueOne = Bytes.toBytes("valueOne");
    byte [] valueTwo = Bytes.toBytes("valueTwo");

    assertNull(this.table.get(row, COL, RP_MAX));

    // compare and swap from null to valueOne
    try {
      assertFalse(
          this.table.compareAndSwap(row, COL, valueOne, valueTwo, RP_MAX, 2L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    try {
      assertTrue(
          this.table.compareAndSwap(row, COL, null, valueOne, RP_MAX, 2L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    try {
      assertTrue(
          this.table.compareAndSwap(row, COL, valueOne, valueTwo, RP_MAX, 3L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    try {
      assertFalse(
          this.table.compareAndSwap(row, COL, valueOne, valueTwo, RP_MAX, 4L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    assertEquals(Bytes.toString(valueTwo),
        Bytes.toString(this.table.get(row, COL, RP_MAX)));

    try {
      assertFalse(
          this.table.compareAndSwap(row, COL, null, valueTwo, RP_MAX, 5L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    try {
      assertTrue(
          this.table.compareAndSwap(row, COL, valueTwo, null, RP_MAX, 5L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    byte [] val = this.table.get(row, COL, RP_MAX);
    assertNull("expected null but was " + Bytes.toString(val), val);
  }

  @Test
  public void testCompareAndSwapsWithReadWritePointers() {

    byte [] row = Bytes.toBytes("testCompareAndSwapsWithReadWritePointers");

    byte [] valueOne = Bytes.toBytes("valueOne");
    byte [] valueTwo = Bytes.toBytes("valueTwo");

    assertNull(this.table.get(row, COL, RP_MAX));

    // compare and swap null to valueOne, max to v2

    try {
      assertTrue(
          this.table.compareAndSwap(row, COL, null, valueOne, RP_MAX, 2L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    // null to valueTwo, read v1 write v3

    try {
      assertTrue(
          this.table.compareAndSwap(row, COL, null, valueTwo, new MemoryReadPointer(1L), 3L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    // read @ 3 gives value two

    assertEquals(Bytes.toString(valueTwo),
        Bytes.toString(this.table.get(row, COL, new MemoryReadPointer(3))));

    // read @ 2 gives value one

    assertEquals(Bytes.toString(valueOne),
        Bytes.toString(this.table.get(row, COL, new MemoryReadPointer(2))));

    // cas valueOne @ ts2 to valueTwo @ ts4

    try {
      assertTrue(
          this.table.compareAndSwap(row, COL, valueOne, valueTwo, new MemoryReadPointer(2L), 4L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    // cas valueTwo @ ts3 to valueOne @ ts5

    try {
      assertTrue(
          this.table.compareAndSwap(row, COL, valueTwo, valueOne, new MemoryReadPointer(3L), 5L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    // read @ 5 gives value one

    assertEquals(Bytes.toString(valueOne),
        Bytes.toString(this.table.get(row, COL, new MemoryReadPointer(5))));

    // read @ 4 gives value two

    assertEquals(Bytes.toString(valueTwo),
        Bytes.toString(this.table.get(row, COL, new MemoryReadPointer(4))));

    // cas valueTwo @ ts5 to valueOne @ ts6 FAIL

    try {
      assertFalse(
          this.table.compareAndSwap(row, COL, valueTwo, valueOne, new MemoryReadPointer(5L), 6L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    // cas valueOne @ ts4 to valueTwo @ ts6 FAIL

    try {
      assertFalse(
          this.table.compareAndSwap(row, COL, valueOne, valueTwo, new MemoryReadPointer(4L), 6L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    // cas valueOne@5 to valueTwo@5

    assertEquals(Bytes.toString(valueOne),
        Bytes.toString(this.table.get(row, COL, new MemoryReadPointer(5))));
    try {
      assertTrue(
          this.table.compareAndSwap(row, COL, valueOne, valueTwo,
              new MemoryReadPointer(5L), 5L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    assertEquals(Bytes.toString(valueTwo),
        Bytes.toString(this.table.get(row, COL, new MemoryReadPointer(5))));

  }

  @Test
  public void testIncrementsSupportReadAndWritePointers() {

    byte [] row = Bytes.toBytes("testIncrementsSupportReadAndWritePointers");

    // increment with write pointers

    assertEquals(1L, this.table.increment(row, COL, 1L, RP_MAX, 1L));

    assertEquals(1L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(1L))));
    assertEquals(1L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(2L))));
    assertEquals(1L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(3L))));
    assertEquals(1L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(4L))));

    assertEquals(3L, this.table.increment(row, COL, 2L, RP_MAX, 3L));

    assertEquals(1L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(1L))));
    assertEquals(1L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(2L))));
    assertEquals(3L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(3L))));
    assertEquals(3L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(4L))));

    // test an increment with a read pointer

    assertEquals(2L, this.table.increment(row, COL, 1L,
        new MemoryReadPointer(1L), 2L));

    // read it back with read pointer reads

    assertEquals(3L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(3L))));
    assertEquals(2L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(2L))));
    assertEquals(1L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(1L))));

    // read it back with increment=0

    assertEquals(1L, this.table.increment(row, COL, 0L,
        new MemoryReadPointer(1L), 1L));
    assertEquals(2L, this.table.increment(row, COL, 0L,
        new MemoryReadPointer(2L), 2L));
    assertEquals(3L, this.table.increment(row, COL, 0L,
        new MemoryReadPointer(3L), 3L));
  }

  @Test
  public void testIncrementCASIncrementWithSameTimestamp() {
    byte [] row = Bytes.toBytes("testICASIWSTS");

    // increment with same read and write pointer
    
    assertEquals(4L, this.table.increment(row, COL, 4L,
        new MemoryReadPointer(3L), 3L));
    assertEquals(4L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(3L))));
    
    // cas from 4 to 6 @ ts3

    try {
      assertTrue(this.table.compareAndSwap(row, COL, Bytes.toBytes(4L),
          Bytes.toBytes(6L), new MemoryReadPointer(3L), 3L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    assertEquals(6L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(3L))));
    
    // increment to 7 @ ts3

    assertEquals(7L, this.table.increment(row, COL, 1L,
        new MemoryReadPointer(3L), 3L));
    assertEquals(7L, Bytes.toLong(
        this.table.get(row, COL, new MemoryReadPointer(3L))));
    
  }
  @Test
  public void testSameVersionOverwritesExisting() {

    byte [] row = Bytes.toBytes("testSVOEKey");

    // Write value = 5 @ ts = 5
    this.table.put(row, COL, 5L, Bytes.toBytes(5L));

    // Read value = 5 @ tsMax
    assertEquals(5L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));

    // Write value = 10 @ ts = 10
    this.table.put(row, COL, 10L, Bytes.toBytes(10L));

    // Read value = 10 @ tsMax
    assertEquals(10L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));

    // Write value = 11 @ ts = 10
    this.table.put(row, COL, 10L, Bytes.toBytes(11L));

    // Read value = 11 @ tsMax
    assertEquals(11L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));

    // Read value = 11 @ ts <= 10
    assertEquals(11L, Bytes.toLong(this.table.get(row, COL,
        new MemoryReadPointer(10L))));

    // Read value = 5 @ ts <= 9
    assertEquals(5L, Bytes.toLong(this.table.get(row, COL,
        new MemoryReadPointer(9L))));

    // Increment + 1 @ ts = 9 to ts=11
    assertEquals(6L, this.table.increment(row, COL, 1L,
        new MemoryReadPointer(9L), 11L));

    // Read value = 6 @ tsMax
    assertEquals(6L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));

    // CompareAndSwap 6 to 15 @ ts = 11
    try {
      assertTrue(this.table.compareAndSwap(row, COL, Bytes.toBytes(6L),
          Bytes.toBytes(15L), new MemoryReadPointer(11L), 12L));
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    // Increment + 1 @ ts = 12
    assertEquals(16L, this.table.increment(row, COL, 1L,
        new MemoryReadPointer(12L), 13L));

    // Read value = 16 @ tsMax
    assertEquals(16L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));

    // Read value = 5 @ ts <= 9
    assertEquals(5L, Bytes.toLong(this.table.get(row, COL,
        new MemoryReadPointer(9L))));
  }

  @Test
  public void testDeleteBehavior() {

    byte [] row = Bytes.toBytes("testDeleteBehavior");

    // Verify row dne
    assertNull(this.table.get(row, COL, RP_MAX));

    // Write values 1, 2, 3 @ ts 1, 2, 3
    this.table.put(row, COL, 1L, Bytes.toBytes(1L));
    this.table.put(row, COL, 3L, Bytes.toBytes(3L));
    this.table.put(row, COL, 2L, Bytes.toBytes(2L));

    // Read value, should be 3
    assertEquals(3L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));

    // Point delete at 2
    this.table.delete(row, COL, 2L);

    // Read value, should be 3
    assertEquals(3L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));

    // Point delete at 3
    this.table.delete(row, COL, 3L);

    // Read value, should be 1 (2 and 3 point deleted)
    assertEquals(1L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));

    // DeleteAll at 3
    this.table.deleteAll(row, COL, 3L);

    // Read value, should not exist
    assertNull(this.table.get(row, COL, RP_MAX));

    // Write at 3 (trying to overwrite existing deletes @ 3)
    this.table.put(row, COL, 3L, Bytes.toBytes(3L));

    // Read value
    // If writes can overwrite deletes at the same timestamp:
    // assertEquals(3L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));
    // Currently, a delete cannot be overwritten on the same version:
    assertNull(this.table.get(row, COL, RP_MAX));

    // Undelete the delete all at 3
    this.table.undeleteAll(row, COL, 3L);

    // There is still a point delete at 3, should uncover 1
    assertEquals(1L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));

    // DeleteAll at 5
    this.table.deleteAll(row, COL, 5L);

    // Read value, should not exist
    assertNull(this.table.get(row, COL, RP_MAX));

    // Write at 4
    this.table.put(row, COL, 4L, Bytes.toBytes(4L));

    // Read value, should not exist
    assertNull(this.table.get(row, COL, RP_MAX));

    // Write at 6
    this.table.put(row, COL, 6L, Bytes.toBytes(6L));

    // Read value, should be 6
    assertEquals(6L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));

    // Undelete the delete all at 5
    this.table.undeleteAll(row, COL, 5L);

    // 6 still visible
    assertEquals(6L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));

    // 4 is still visible at ts=4
    assertEquals(4L, Bytes.toLong(this.table.get(row, COL, new MemoryReadPointer(4))));

    // Point delete 6
    this.table.delete(row, COL, 6L);

    // Read value, should now be 4
    assertEquals(4L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));

  }

  @Test
  public void testGetAllKeys() {

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
}
