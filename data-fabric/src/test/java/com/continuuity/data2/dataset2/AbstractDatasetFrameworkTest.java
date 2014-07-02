package com.continuuity.data2.dataset2;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.data2.dataset2.lib.table.CoreDatasetsModule;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryOrderedTableModule;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.inmemory.MinimalTxSystemClient;
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

    Assert.assertFalse(framework.hasType("orderedTable"));
    framework.addModule(moduleName, new InMemoryOrderedTableModule());
    Assert.assertTrue(framework.hasType("orderedTable"));

    Assert.assertFalse(framework.hasInstance("my_table"));
    // Creating instance
    framework.addInstance("orderedTable", "my_table", DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance("my_table"));

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

    framework.addModule("inMemory", new InMemoryOrderedTableModule());
    framework.addModule("core", new CoreDatasetsModule());
    Assert.assertFalse(framework.hasType(SimpleKVTable.class.getName()));
    framework.addModule("keyValue", new SingleTypeModule(SimpleKVTable.class));
    Assert.assertTrue(framework.hasType(SimpleKVTable.class.getName()));

    // Creating instance
    Assert.assertFalse(framework.hasInstance("my_table"));
    framework.addInstance(SimpleKVTable.class.getName(),
                          "my_table", DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance("my_table"));

    testCompositeDataset(framework);

    // cleanup
    framework.deleteInstance("my_table");
    framework.deleteModule("keyValue");
    framework.deleteModule("core");
    framework.deleteModule("inMemory");
  }

  @Test
  public void testDoubleCompositeDataset() throws Exception {
    // Configuring Dataset types
    DatasetFramework framework = getFramework();

    framework.addModule("inMemory", new InMemoryOrderedTableModule());
    framework.addModule("core", new CoreDatasetsModule());
    framework.addModule("keyValue", new SingleTypeModule(SimpleKVTable.class));
    Assert.assertFalse(framework.hasType(DoubleWrappedKVTable.class.getName()));
    framework.addModule("doubleKeyValue", new SingleTypeModule(DoubleWrappedKVTable.class));
    Assert.assertTrue(framework.hasType(DoubleWrappedKVTable.class.getName()));

    // Creating instance
    Assert.assertFalse(framework.hasInstance("my_table"));
    framework.addInstance(DoubleWrappedKVTable.class.getName(), "my_table", DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance("my_table"));

    testCompositeDataset(framework);

    // cleanup
    framework.deleteInstance("my_table");
    framework.deleteModule("doubleKeyValue");
    framework.deleteModule("keyValue");
    framework.deleteModule("core");
    framework.deleteModule("inMemory");
  }

  private void testCompositeDataset(DatasetFramework framework) throws Exception {

    // Doing some admin and data ops
    DatasetAdmin admin = framework.getAdmin("my_table", null);
    Assert.assertNotNull(admin);
    final KeyValueTable table = framework.getDataset("my_table", null);
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
  }

  @Test
  public void testBasicManagement() throws Exception {
    // Adding modules
    DatasetFramework framework = getFramework();

    framework.addModule("inMemory", new InMemoryOrderedTableModule());
    framework.addModule("core", new CoreDatasetsModule());
    Assert.assertTrue(framework.hasType(OrderedTable.class.getName()));
    Assert.assertTrue(framework.hasType(Table.class.getName()));

    // Creating instances
    framework.addInstance(OrderedTable.class.getName(), "my_table", DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance("my_table"));
    DatasetSpecification spec = framework.getDatasetSpec("my_table");
    Assert.assertNotNull(spec);
    Assert.assertEquals("my_table", spec.getName());
    Assert.assertEquals(OrderedTable.class.getName(), spec.getType());
    framework.addInstance(OrderedTable.class.getName(), "my_table2", DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance("my_table2"));

    // cleanup
    try {
      framework.deleteAllModules();
      Assert.fail("should not delete modules: there are datasets using their types");
    } catch (DatasetManagementException e) {
      // expected
    }
    // types are still there
    Assert.assertTrue(framework.hasType(OrderedTable.class.getName()));
    Assert.assertTrue(framework.hasType(Table.class.getName()));

    framework.deleteAllInstances();
    Assert.assertEquals(0, framework.getInstances().size());
    Assert.assertFalse(framework.hasInstance("my_table"));
    Assert.assertNull(framework.getDatasetSpec("my_table"));
    Assert.assertFalse(framework.hasInstance("my_table2"));
    Assert.assertNull(framework.getDatasetSpec("my_table2"));

    // now it should susceed
    framework.deleteAllModules();
    Assert.assertFalse(framework.hasType(OrderedTable.class.getName()));
    Assert.assertFalse(framework.hasType(Table.class.getName()));
  }

}
