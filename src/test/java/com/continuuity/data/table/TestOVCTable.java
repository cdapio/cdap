package com.continuuity.data.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;

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
  public void testSimpleIncrement() {

    byte [] row = Bytes.toBytes("testSimpleIncrement");

    assertEquals(1L, this.table.increment(row, COL, 1L, RP_MAX, 1L));

    assertEquals(3L, this.table.increment(row, COL, 2L, RP_MAX, 2L));

    assertEquals(3L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));

  }

  @Test
  public void testSimpleCompareAndSwap() {

    byte [] row = Bytes.toBytes("testSimpleCompareAndSwap");

    byte [] valueOne = Bytes.toBytes("valueOne");
    byte [] valueTwo = Bytes.toBytes("valueTwo");

    this.table.put(row, COL, 1L, valueOne);

    assertTrue(
        this.table.compareAndSwap(row, COL, valueOne, valueTwo, RP_MAX, 2L));

    assertFalse(
        this.table.compareAndSwap(row, COL, valueOne, valueTwo, RP_MAX, 3L));

    assertEquals(Bytes.toString(valueTwo),
        Bytes.toString(this.table.get(row, COL, RP_MAX)));
    assertEquals(Bytes.toString(valueTwo),
        Bytes.toString(this.table.get(row, COL, new MemoryReadPointer(2L))));

    assertTrue(
        this.table.compareAndSwap(row, COL, valueTwo, valueOne, RP_MAX, 2L));

  }

  @Test
  public void testIncrementsSupportReadAndWritePointers() {

    byte [] row = Bytes.toBytes("testIncrementsSupportReadAndWritePointers");

    assertEquals(1L, this.table.increment(row, COL, 1L, RP_MAX, 1L));

    assertEquals(3L, this.table.increment(row, COL, 2L, RP_MAX, 2L));

    assertEquals(3L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));

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

    // Increment + 1 @ ts = 10
    assertEquals(12L, this.table.increment(row, COL, 1L,
        new MemoryReadPointer(9L, 10L, null), 10L));

    // Read value = 12 @ tsMax
    assertEquals(12L, Bytes.toLong(this.table.get(row, COL, RP_MAX)));

    // CompareAndSwap 12 to 15 @ ts = 10
    assertTrue(this.table.compareAndSwap(row, COL, Bytes.toBytes(12L),
        Bytes.toBytes(15L), new MemoryReadPointer(9L, 10L, null), 10L));

    // Increment + 1 @ ts = 10
    assertEquals(16L, this.table.increment(row, COL, 1L,
        new MemoryReadPointer(9L, 10L, null), 10L));

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
    this.table.undeleteAll(Bytes.toBytes("row" + 6), COL, 10);

    // get all keys and only get 8 back
    keys = this.table.getKeys(Integer.MAX_VALUE, 0, RP_MAX);
    assertEquals(8, keys.size());

  }
}
