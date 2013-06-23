package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.engine.memory.MemoryOVCTable;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMemoryOVCTable extends TestOVCTable {

  private static final Injector injector =
      Guice.createInjector(new DataFabricModules().getInMemoryModules());

  @Override
  protected OVCTableHandle injectTableHandle() {
    return injector.getInstance(OVCTableHandle.class);
  }

  @Test
  public void testDeleteDirty() throws OperationException {
    byte [] row = Bytes.toBytes("testDeleteDirty");

    // Verify row dne
    assertTrue(this.table.get(row, COL, RP_MAX).isEmpty());

    // Write values 1, 2, 3 @ ts 1, 2, 3
    this.table.put(row, COL, 1L, Bytes.toBytes(1L));
    this.table.put(row, COL, 3L, Bytes.toBytes(3L));
    this.table.put(row, COL, 2L, Bytes.toBytes(2L));

    // Read value, should be 3
    assertEquals(3L, Bytes.toLong(this.table.get(row, COL, new MemoryReadPointer(3)).getValue()));

    // full delete at 2
    this.table.deleteAll(row, COL, 2L);

    // Read value with read pointer 3 should still be 3
    assertEquals(3L, Bytes.toLong(this.table.get(row, COL, new MemoryReadPointer(3)).getValue()));
    // with read pointer 2 should be gone
    assertTrue(this.table.get(row, COL, new MemoryReadPointer(2)).isEmpty());
    // but with read pointer 1 should still be 1
    assertEquals(1L, Bytes.toLong(this.table.get(row, COL, new MemoryReadPointer(1)).getValue()));

    // dirty delete at 2
    this.table.deleteDirty(row, new byte[][] { COL }, 2L);

    // Read value with read pointer 3 should still be 3
    assertEquals(3L, Bytes.toLong(this.table.get(row, COL, new MemoryReadPointer(3)).getValue()));
    // with read pointer 2 should be gone, and also with read pointer 1
    assertTrue(this.table.get(row, COL, new MemoryReadPointer(2)).isEmpty());
    assertTrue(this.table.get(row, COL, new MemoryReadPointer(1)).isEmpty());

    // dirty delete at 4
    this.table.deleteDirty(row, new byte[][] { COL }, 4L);

    // now it should be gone for all read pointers
    assertTrue(this.table.get(row, COL, new MemoryReadPointer(3)).isEmpty());
    assertTrue(this.table.get(row, COL, new MemoryReadPointer(2)).isEmpty());
    assertTrue(this.table.get(row, COL, new MemoryReadPointer(1)).isEmpty());
  }

  @Override
  public void testInjection() {
    assertTrue(table instanceof MemoryOVCTable);
  }
}
