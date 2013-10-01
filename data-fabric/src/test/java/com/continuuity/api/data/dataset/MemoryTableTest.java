package com.continuuity.api.data.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.dataset.table.Get;
import com.continuuity.api.data.dataset.table.MemoryTable;
import com.continuuity.api.data.dataset.table.Put;
import com.continuuity.data.dataset.DataSetTestBase;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableService;
import com.continuuity.data2.transaction.TransactionContext;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

/**
 * MemoryTable tets
 */
public class MemoryTableTest extends DataSetTestBase {
  @Test
  public void testMemoryTable() throws Exception {
    // Just a basic test:
    // 1) simple in-tx read-write
    // 2) new instance via new instantiator is empty: no state persisted
    // 3) basic tx rollback works

    // 1)...
    DataSet memoryTable = new MemoryTable("memoryTable");
    setupInstantiator(ImmutableList.of(memoryTable));
    MemoryTable table = instantiator.getDataSet("memoryTable");

    TransactionContext txContext = newTransaction();

    table.put(new Put("key1", "col1", "val1"));
    Assert.assertEquals("val1", table.get(new Get("key1", "col1")).getString("col1"));

    txContext.finish();

    // 2)...
    // reset will clean in-memory state
    InMemoryOcTableService.reset();
    setupInstantiator(ImmutableList.of(memoryTable));
    table = instantiator.getDataSet("memoryTable");

    txContext = newTransaction();

    Assert.assertNull(table.get(new Get("key1", "col1")).getString("col1"));

    // 3)...
    table.put(new Put("key1", "col1", "val1"));
    Assert.assertEquals("val1", table.get(new Get("key1", "col1")).getString("col1"));

    txContext.finish();

    txContext = newTransaction();

    Assert.assertEquals("val1", table.get(new Get("key1", "col1")).getString("col1"));
    table.put(new Put("key1", "col1", "val2"));
    Assert.assertEquals("val2", table.get(new Get("key1", "col1")).getString("col1"));

    txContext.abort();

    Assert.assertEquals("val1", table.get(new Get("key1", "col1")).getString("col1"));
  }

}
