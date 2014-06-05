package com.continuuity.data2.dataset2;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.inmemory.MinimalTxSystemClient;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.lib.table.OrderedTable;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public abstract class AbstractDatasetFrameworkTest {
  protected abstract DatasetFramework getFramework();

  @Test
  public void testSimpleDataset() throws Exception {
    // Configuring Dataset types
    DatasetFramework framework = getFramework();
    String moduleName = "inMemory";

    framework.register(moduleName, InMemoryTableModule.class);

    // Creating instance
    framework.addInstance("orderedTable", "my_table", DatasetInstanceProperties.EMPTY);

    // Doing some admin and data ops
    DatasetAdmin admin = framework.getAdmin("my_table", null);
    Assert.assertNotNull(admin);
    final OrderedTable table = framework.getDataset("my_table", null);
    Assert.assertNotNull(table);

    TransactionExecutor txnl = new DefaultTransactionExecutor(new MinimalTxSystemClient(), (TransactionAware) table);
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        table.put(Bytes.toBytes("key1"), Bytes.toBytes("column1"), Bytes.toBytes("value1"));
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals("value1", Bytes.toString(table.get(Bytes.toBytes("key1"), Bytes.toBytes("column1"))));
      }
    });
    admin.truncate();
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertTrue(table.get(Bytes.toBytes("key1")).isEmpty());
      }
    });

    // cleanup
    framework.deleteInstance("my_table");
    framework.deleteModule("inMemory");
  }

  @Test
  public void testCompositeDataset() throws Exception {
    // Configuring Dataset types
    DatasetFramework framework = getFramework();

    framework.register("inMemory", InMemoryTableModule.class);
    framework.register("keyValue", KeyValueTableDefinition.KeyValueTableModule.class);

    // Creating instance
    framework.addInstance("keyValueTable", "my_table", DatasetInstanceProperties.EMPTY);

    // Doing some admin and data ops
    DatasetAdmin admin = framework.getAdmin("my_table", null);
    Assert.assertNotNull(admin);
    final KeyValueTableDefinition.KeyValueTable table = framework.getDataset("my_table", null);
    Assert.assertNotNull(table);

    TransactionExecutor txnl = new DefaultTransactionExecutor(new MinimalTxSystemClient(), (TransactionAware) table);
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        table.put("key1", "value1");
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals("value1", table.get("key1"));
      }
    });
    admin.truncate();
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(null, table.get("key1"));
      }
    });

    // cleanup
    framework.deleteInstance("my_table");
    framework.deleteModule("keyValue");
    framework.deleteModule("inMemory");
  }

}
