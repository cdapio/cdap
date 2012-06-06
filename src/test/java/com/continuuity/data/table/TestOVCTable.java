package com.continuuity.data.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.runtime.DataFabricInMemoryModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Tests the contract and semantics of {@link OrderedVersionedColumnarTable}
 * against each of the implementations.
 */
public class TestOVCTable {

  // TODO: As part of ENG-211, add testing of HBaseOVCTable
  // TODO: As part of ENG-272, add testing of HyperSQLOVCTable

  private OVCTableHandle tableHandle;
  private OrderedVersionedColumnarTable table;

  @Before
  public void initialize() {
    Injector injector = Guice.createInjector(new DataFabricInMemoryModule());
    tableHandle = injector.getInstance(OVCTableHandle.class);
    table = tableHandle.getTable(Bytes.toBytes("TestOVCTable"));
  }

  private static final byte [] COL = new byte [] { (byte)0 };
  private static final MemoryReadPointer RP_MAX =
      new MemoryReadPointer(Long.MAX_VALUE);
  
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

//  @Test
//  public void testDeleteBehavior() {
//  }
}
