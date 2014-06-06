package com.continuuity.data2.dataset2;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.dataset2.lib.table.KeyValueTable;
import com.continuuity.data2.dataset2.lib.table.KeyValueTableModule;
import com.continuuity.data2.dataset2.lib.table.ObjectStore;
import com.continuuity.data2.dataset2.lib.table.ObjectStoreModule;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.inmemory.MinimalTxSystemClient;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.lib.table.OrderedTable;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.TypeRepresentation;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public abstract class AbstractDatasetFrameworkTest {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDatasetFrameworkTest.class);
  private static final Gson GSON = new Gson();

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
    framework.register("keyValue", KeyValueTableModule.class);

    // Creating instance
    framework.addInstance("keyValueTable", "my_table", DatasetInstanceProperties.EMPTY);

    // Doing some admin and data ops
    DatasetAdmin admin = framework.getAdmin("my_table", null);
    Assert.assertNotNull(admin);
    final KeyValueTable table = framework.getDataset("my_table", null);
    Assert.assertNotNull(table);

    TransactionExecutor txnl = new DefaultTransactionExecutor(new MinimalTxSystemClient(), (TransactionAware) table);
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        table.write("key1", "value1");
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals("value1", table.read("key1"));
      }
    });
    admin.truncate();
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(null, table.read("key1"));
      }
    });

    // cleanup
    framework.deleteInstance("my_table");
    framework.deleteModule("keyValue");
    framework.deleteModule("inMemory");
  }

  @Test
  public void testObjectStore() throws Exception {
    // Configuring Dataset types
    DatasetFramework framework = getFramework();

    framework.register("inMemory", InMemoryTableModule.class);
    framework.register("keyValue", KeyValueTableModule.class);
    framework.register("objectStore", ObjectStoreModule.class);

    // Creating instance
    TypeRepresentation typeRep = new TypeRepresentation(new TypeToken<List<TestObject>>() { }.getType());
    Schema schema = new ReflectionSchemaGenerator().generate(typeRep);
    framework.addInstance("objectStore", "my_test_objectstore",
                          new DatasetInstanceProperties.Builder()
                            .property("type", GSON.toJson(typeRep))
                            .property("schema", GSON.toJson(schema))
                            .build());

    // Doing some admin and data ops
    DatasetAdmin admin = framework.getAdmin("my_test_objectstore", null);
    Assert.assertNotNull(admin);
    final ObjectStore<List<TestObject>> table = framework.getDataset("my_test_objectstore", null);
    Assert.assertNotNull(table);

    final ArrayList<TestObject> testValue = Lists.newArrayList(
      new TestObject("123", 321L), new TestObject("sdf", 333L));

    TransactionExecutor txnl = new DefaultTransactionExecutor(new MinimalTxSystemClient(), (TransactionAware) table);

    // test simple write
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        table.write("key1", testValue);
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertArrayEquals(testValue.toArray(), table.read("key1").toArray());
      }
    });

    // test simple truncate
    admin.truncate();
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(null, table.read("key1"));
      }
    });

    // cleanup
    framework.deleteInstance("my_test_objectstore");
    framework.deleteModule("objectStore");
    framework.deleteModule("keyValue");
    framework.deleteModule("inMemory");
  }

}
