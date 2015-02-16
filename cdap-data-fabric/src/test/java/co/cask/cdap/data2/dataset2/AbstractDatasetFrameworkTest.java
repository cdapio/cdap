/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryOrderedTableModule;
import co.cask.cdap.proto.Id;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.inmemory.MinimalTxSystemClient;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public abstract class AbstractDatasetFrameworkTest {

  protected abstract DatasetFramework getFramework();

  private static final Id.Namespace namespaceId = Id.Namespace.from("myspace");
  private static final Id.DatasetModule inMemory = Id.DatasetModule.from(namespaceId, "inMemory");
  private static final Id.DatasetModule core = Id.DatasetModule.from(namespaceId, "core");
  private static final Id.DatasetModule keyValue = Id.DatasetModule.from(namespaceId, "keyValue");
  private static final Id.DatasetModule doubleKeyValue = Id.DatasetModule.from(namespaceId, "doubleKeyValue");

  @Test
  public void testSimpleDataset() throws Exception {
    // Configuring Dataset types
    DatasetFramework framework = getFramework();

    Assert.assertFalse(framework.hasType("orderedTable"));
    framework.addModule(inMemory, new InMemoryOrderedTableModule());
    Assert.assertTrue(framework.hasType("orderedTable"));

    Assert.assertFalse(framework.hasInstance("my_table"));
    // Creating instance
    framework.addInstance("orderedTable", "my_table", DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance("my_table"));

    // Doing some admin and data ops
    DatasetAdmin admin = framework.getAdmin("my_table", null);
    Assert.assertNotNull(admin);
    final OrderedTable table = framework.getDataset("my_table", DatasetDefinition.NO_ARGUMENTS, null);
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
    framework.deleteModule(inMemory);
  }

  @Test
  public void testCompositeDataset() throws Exception {
    // Configuring Dataset types
    DatasetFramework framework = getFramework();

    framework.addModule(inMemory, new InMemoryOrderedTableModule());
    framework.addModule(core, new CoreDatasetsModule());
    Assert.assertFalse(framework.hasType(SimpleKVTable.class.getName()));
    framework.addModule(keyValue, new SingleTypeModule(SimpleKVTable.class));
    Assert.assertTrue(framework.hasType(SimpleKVTable.class.getName()));

    // Creating instance
    Assert.assertFalse(framework.hasInstance("my_table"));
    framework.addInstance(SimpleKVTable.class.getName(),
                          "my_table", DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance("my_table"));

    testCompositeDataset(framework);

    // cleanup
    framework.deleteInstance("my_table");
    framework.deleteModule(keyValue);
    framework.deleteModule(core);
    framework.deleteModule(inMemory);
  }

  @Test
  public void testDoubleCompositeDataset() throws Exception {
    // Configuring Dataset types
    DatasetFramework framework = getFramework();

    framework.addModule(inMemory, new InMemoryOrderedTableModule());
    framework.addModule(core, new CoreDatasetsModule());
    framework.addModule(keyValue, new SingleTypeModule(SimpleKVTable.class));
    Assert.assertFalse(framework.hasType(DoubleWrappedKVTable.class.getName()));
    framework.addModule(doubleKeyValue, new SingleTypeModule(DoubleWrappedKVTable.class));
    Assert.assertTrue(framework.hasType(DoubleWrappedKVTable.class.getName()));

    // Creating instance
    Assert.assertFalse(framework.hasInstance("my_table"));
    framework.addInstance(DoubleWrappedKVTable.class.getName(), "my_table", DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance("my_table"));

    testCompositeDataset(framework);

    // cleanup
    framework.deleteInstance("my_table");
    framework.deleteModule(doubleKeyValue);
    framework.deleteModule(keyValue);
    framework.deleteModule(core);
    framework.deleteModule(inMemory);
  }

  private void testCompositeDataset(DatasetFramework framework) throws Exception {

    // Doing some admin and data ops
    DatasetAdmin admin = framework.getAdmin("my_table", null);
    Assert.assertNotNull(admin);
    final KeyValueTable table = framework.getDataset("my_table", DatasetDefinition.NO_ARGUMENTS, null);
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

    framework.addModule(inMemory, new InMemoryOrderedTableModule());
    framework.addModule(core, new CoreDatasetsModule());
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
