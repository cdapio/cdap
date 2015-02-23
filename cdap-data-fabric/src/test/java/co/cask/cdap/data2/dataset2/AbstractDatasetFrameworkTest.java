/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryOrderedTableModule;
import co.cask.cdap.proto.Id;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.inmemory.MinimalTxSystemClient;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 *
 */
public abstract class AbstractDatasetFrameworkTest {

  protected abstract DatasetFramework getFramework();

  protected static final Map<String, DatasetModule> DEFAULT_MODULES;
  static {
    DEFAULT_MODULES = Maps.newLinkedHashMap();
    DEFAULT_MODULES.put("orderedTable-memory", new InMemoryOrderedTableModule());
    DEFAULT_MODULES.put("core", new CoreDatasetsModule());
  }

  protected static final Id.Namespace NAMESPACE_ID = Id.Namespace.from("myspace");
  private static final Id.DatasetModule inMemory = Id.DatasetModule.from(NAMESPACE_ID, "inMemory");
  private static final Id.DatasetModule core = Id.DatasetModule.from(NAMESPACE_ID, "core");
  private static final Id.DatasetModule keyValue = Id.DatasetModule.from(NAMESPACE_ID, "keyValue");
  private static final Id.DatasetModule doubleKeyValue = Id.DatasetModule.from(NAMESPACE_ID, "doubleKeyValue");
  private static final Id.DatasetInstance myTable = Id.DatasetInstance.from(NAMESPACE_ID, "my_table");
  private static final Id.DatasetInstance myTable2 = Id.DatasetInstance.from(NAMESPACE_ID, "my_table2");
  private static final Id.DatasetType inMemoryType = Id.DatasetType.from(NAMESPACE_ID, "orderedTable");
  private static final Id.DatasetType tableType = Id.DatasetType.from(NAMESPACE_ID, "table");
  private static final Id.DatasetType simpleKvType = Id.DatasetType.from(NAMESPACE_ID, SimpleKVTable.class.getName());
  private static final Id.DatasetType doubleKvType = Id.DatasetType.from(NAMESPACE_ID,
                                                                         DoubleWrappedKVTable.class.getName());

  @Test
  public void testSimpleDataset() throws Exception {
    // Configuring Dataset types
    DatasetFramework framework = getFramework();
    // system namespace has a module orderedTable-inMemory
    Assert.assertTrue(framework.hasSystemType("orderedTable"));
    // myspace namespace has no modules
    Assert.assertFalse(framework.hasType(inMemoryType));
    Assert.assertFalse(framework.hasType(simpleKvType));
    // add module to namespace 'myspace'
    framework.addModule(keyValue, new SingleTypeModule(SimpleKVTable.class));
    // make sure it got added to 'myspace'
    Assert.assertTrue(framework.hasType(simpleKvType));
    // but not to 'system'
    Assert.assertFalse(framework.hasSystemType(SimpleKVTable.class.getName()));

    Assert.assertFalse(framework.hasInstance(myTable));
    // Creating instance using a type from own namespace
    framework.addInstance(SimpleKVTable.class.getName(), myTable, DatasetProperties.EMPTY);
    // verify it got added to the right namespace
    Assert.assertTrue(framework.hasInstance(myTable));
    // and not to the system namespace
    Assert.assertFalse(framework.hasInstance(Id.DatasetInstance.from(Constants.SYSTEM_NAMESPACE, "my_table")));

    // Doing some admin and data ops
    DatasetAdmin admin = framework.getAdmin(myTable, null);
    Assert.assertNotNull(admin);
    final SimpleKVTable table = framework.getDataset(myTable, DatasetDefinition.NO_ARGUMENTS, null);
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
        Assert.assertTrue(table.get("key1") == null);
      }
    });

    // cleanup
    framework.deleteInstance(myTable);
    framework.deleteModule(keyValue);

    // recreate instance without adding a module in 'myspace'. This should use types from default namespace
    framework.addInstance("orderedTable", myTable, DatasetProperties.EMPTY);
    // verify it got added to the right namespace
    Assert.assertTrue(framework.hasInstance(myTable));
    admin = framework.getAdmin(myTable, null);
    Assert.assertNotNull(admin);
    final OrderedTable orderedTable = framework.getDataset(myTable, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(orderedTable);
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        orderedTable.put(Bytes.toBytes("key1"), Bytes.toBytes("column1"), Bytes.toBytes("value1"));
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals("value1", Bytes.toString(orderedTable.get(Bytes.toBytes("key1"),
                                                                      Bytes.toBytes("column1"))));
      }
    });

    // cleanup
    framework.deleteInstance(myTable);
  }

  @Test
  public void testCompositeDataset() throws Exception {
    // Configuring Dataset types
    DatasetFramework framework = getFramework();

    framework.addModule(inMemory, new InMemoryOrderedTableModule());
    framework.addModule(core, new CoreDatasetsModule());
    Assert.assertFalse(framework.hasSystemType(SimpleKVTable.class.getName()));
    Assert.assertFalse(framework.hasType(simpleKvType));
    framework.addModule(keyValue, new SingleTypeModule(SimpleKVTable.class));
    Assert.assertTrue(framework.hasType(simpleKvType));
    Assert.assertFalse(framework.hasSystemType(SimpleKVTable.class.getName()));

    // Creating instance
    Assert.assertFalse(framework.hasInstance(myTable));
    framework.addInstance(SimpleKVTable.class.getName(), myTable, DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance(myTable));

    testCompositeDataset(framework);

    // cleanup
    framework.deleteInstance(myTable);
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
    Assert.assertFalse(framework.hasSystemType(DoubleWrappedKVTable.class.getName()));
    Assert.assertFalse(framework.hasType(doubleKvType));
    framework.addModule(doubleKeyValue, new SingleTypeModule(DoubleWrappedKVTable.class));
    Assert.assertTrue(framework.hasType(doubleKvType));
    Assert.assertFalse(framework.hasSystemType(DoubleWrappedKVTable.class.getName()));

    // Creating instance
    Assert.assertFalse(framework.hasInstance(myTable));
    framework.addInstance(DoubleWrappedKVTable.class.getName(), myTable, DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance(myTable));

    testCompositeDataset(framework);

    // cleanup
    framework.deleteInstance(myTable);
    framework.deleteModule(doubleKeyValue);
    framework.deleteModule(keyValue);
    framework.deleteModule(core);
    framework.deleteModule(inMemory);
  }

  private void testCompositeDataset(DatasetFramework framework) throws Exception {

    // Doing some admin and data ops
    DatasetAdmin admin = framework.getAdmin(myTable, null);
    Assert.assertNotNull(admin);
    final KeyValueTable table = framework.getDataset(myTable, DatasetDefinition.NO_ARGUMENTS, null);
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
    Id.DatasetType orderedTableType = Id.DatasetType.from(NAMESPACE_ID, OrderedTable.class.getName());
    Id.DatasetType tableType = Id.DatasetType.from(NAMESPACE_ID, Table.class.getName());

    // Adding modules
    DatasetFramework framework = getFramework();

    framework.addModule(inMemory, new InMemoryOrderedTableModule());
    framework.addModule(core, new CoreDatasetsModule());
    framework.addModule(keyValue, new SingleTypeModule(SimpleKVTable.class));
    // keyvalue has been added in the system namespace
    Assert.assertTrue(framework.hasSystemType(OrderedTable.class.getName()));
    Assert.assertTrue(framework.hasSystemType(Table.class.getName()));
    Assert.assertFalse(framework.hasSystemType(SimpleKVTable.class.getName()));
    Assert.assertTrue(framework.hasType(orderedTableType));
    Assert.assertTrue(framework.hasType(tableType));
    Assert.assertTrue(framework.hasType(simpleKvType));

    // Creating instances
    framework.addInstance(OrderedTable.class.getName(), myTable, DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance(myTable));
    DatasetSpecification spec = framework.getDatasetSpec(myTable);
    Assert.assertNotNull(spec);
    Assert.assertEquals(myTable.getId(), spec.getName());
    Assert.assertEquals(OrderedTable.class.getName(), spec.getType());
    framework.addInstance(OrderedTable.class.getName(), myTable2, DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance(myTable2));

    // cleanup
    try {
      framework.deleteAllModules(NAMESPACE_ID);
      Assert.fail("should not delete modules: there are datasets using their types");
    } catch (DatasetManagementException e) {
      // expected
    }
    // types are still there
    Assert.assertTrue(framework.hasType(orderedTableType));
    Assert.assertTrue(framework.hasType(tableType));
    Assert.assertTrue(framework.hasType(simpleKvType));

    framework.deleteAllInstances(NAMESPACE_ID);
    Assert.assertEquals(0, framework.getInstances(NAMESPACE_ID).size());
    Assert.assertFalse(framework.hasInstance(myTable));
    Assert.assertNull(framework.getDatasetSpec(myTable));
    Assert.assertFalse(framework.hasInstance(myTable2));
    Assert.assertNull(framework.getDatasetSpec(myTable2));

    // now it should succeed
    framework.deleteAllModules(NAMESPACE_ID);
    Assert.assertTrue(framework.hasSystemType(OrderedTable.class.getName()));
    Assert.assertTrue(framework.hasSystemType(Table.class.getName()));
    Assert.assertFalse(framework.hasType(orderedTableType));
    Assert.assertFalse(framework.hasType(tableType));
    Assert.assertFalse(framework.hasType(simpleKvType));
  }

  @Test
  public void testNamespaces() throws DatasetManagementException {
    DatasetFramework framework = getFramework();

    Id.Namespace namespace = Id.Namespace.from("yourspace");
    framework.createNamespace(namespace);
    try {
      framework.createNamespace(namespace);
      Assert.fail("Should not be able to create a duplicate namespace");
    } catch (DatasetManagementException e) {
    }
    framework.deleteNamespace(namespace);
  }
}
