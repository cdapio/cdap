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
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import co.cask.cdap.proto.Id;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.inmemory.MinimalTxSystemClient;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

/**
 *
 */
public abstract class AbstractDatasetFrameworkTest {

  protected abstract DatasetFramework getFramework() throws DatasetManagementException;

  protected static final Map<String, DatasetModule> DEFAULT_MODULES;
  static {
    DEFAULT_MODULES = Maps.newLinkedHashMap();
    DEFAULT_MODULES.put("orderedTable-memory", new InMemoryTableModule());
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
  private static final Id.DatasetType simpleKvType = Id.DatasetType.from(NAMESPACE_ID, SimpleKVTable.class.getName());
  private static final Id.DatasetType doubleKvType = Id.DatasetType.from(NAMESPACE_ID,
                                                                         DoubleWrappedKVTable.class.getName());

  @Test
  public void testSimpleDataset() throws Exception {
    // Configuring Dataset types
    DatasetFramework framework = getFramework();
    // system namespace has a module orderedTable-inMemory
    Assert.assertTrue(framework.hasSystemType("table"));
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
    final SimpleKVTable kvTable = framework.getDataset(myTable, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(kvTable);

    TransactionExecutor txnl = new DefaultTransactionExecutor(new MinimalTxSystemClient(), (TransactionAware) kvTable);
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        kvTable.put("key1", "value1");
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals("value1", kvTable.get("key1"));
      }
    });
    admin.truncate();
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertTrue(kvTable.get("key1") == null);
      }
    });

    // cleanup
    framework.deleteInstance(myTable);
    framework.deleteModule(keyValue);

    // recreate instance without adding a module in 'myspace'. This should use types from default namespace
    framework.addInstance("table", myTable, DatasetProperties.EMPTY);
    // verify it got added to the right namespace
    Assert.assertTrue(framework.hasInstance(myTable));
    admin = framework.getAdmin(myTable, null);
    Assert.assertNotNull(admin);
    final Table table = framework.getDataset(myTable, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(table);
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        table.put(Bytes.toBytes("key1"), Bytes.toBytes("column1"), Bytes.toBytes("value1"));
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals("value1", Bytes.toString(table.get(Bytes.toBytes("key1"),
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

    framework.addModule(inMemory, new InMemoryTableModule());
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

    framework.addModule(inMemory, new InMemoryTableModule());
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
    Id.DatasetType tableType = Id.DatasetType.from(NAMESPACE_ID, Table.class.getName());

    // Adding modules
    DatasetFramework framework = getFramework();

    framework.addModule(inMemory, new InMemoryTableModule());
    framework.addModule(core, new CoreDatasetsModule());
    framework.addModule(keyValue, new SingleTypeModule(SimpleKVTable.class));
    // keyvalue has been added in the system namespace
    Assert.assertTrue(framework.hasSystemType(Table.class.getName()));
    Assert.assertFalse(framework.hasSystemType(SimpleKVTable.class.getName()));
    Assert.assertTrue(framework.hasType(tableType));
    Assert.assertTrue(framework.hasType(simpleKvType));

    // Creating instances
    framework.addInstance(Table.class.getName(), myTable, DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance(myTable));
    DatasetSpecification spec = framework.getDatasetSpec(myTable);
    Assert.assertNotNull(spec);
    Assert.assertEquals(myTable.getId(), spec.getName());
    Assert.assertEquals(Table.class.getName(), spec.getType());
    framework.addInstance(Table.class.getName(), myTable2, DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance(myTable2));

    // cleanup
    try {
      framework.deleteAllModules(NAMESPACE_ID);
      Assert.fail("should not delete modules: there are datasets using their types");
    } catch (DatasetManagementException e) {
      // expected
    }
    // types are still there
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
    Assert.assertTrue(framework.hasSystemType(Table.class.getName()));
    Assert.assertFalse(framework.hasType(tableType));
    Assert.assertFalse(framework.hasType(simpleKvType));
  }

  @Test
  public void testNamespaceCreationDeletion() throws DatasetManagementException {
    DatasetFramework framework = getFramework();

    Id.Namespace namespace = Id.Namespace.from("yourspace");
    framework.createNamespace(namespace);
    try {
      framework.createNamespace(namespace);
      Assert.fail("Should not be able to create a duplicate namespace");
    } catch (DatasetManagementException e) {
      // expected
    }
    framework.deleteNamespace(namespace);
  }

  @Test
  public void testNamespaceInstanceIsolation() throws Exception {
    DatasetFramework framework = getFramework();
    // TODO: CDAP-1562. Have to namespace instances with DefaultDatasetNamespace because the dataset implementations
    // expect the name to be namespaced to avoid conflicts. This is being cleaned up separately, after which this
    // test can be cleaned up to just use the framework directly instead of wrapping it.
    // TODO: Remove this namespacing of names after using DatasetContext in InMemoryTable
    DatasetNamespace dsNamespace = new DefaultDatasetNamespace(CConfiguration.create());

    // create 2 namespaces
    Id.Namespace namespace1 = Id.Namespace.from("ns1");
    Id.Namespace namespace2 = Id.Namespace.from("ns2");
    framework.createNamespace(namespace1);
    framework.createNamespace(namespace2);

    // create 2 tables, one in each namespace. both tables have the same name.
    Id.DatasetInstance table1ID = dsNamespace.namespace(Id.DatasetInstance.from(namespace1, "table"));
    Id.DatasetInstance table2ID = dsNamespace.namespace(Id.DatasetInstance.from(namespace2, "table"));
    // have slightly different properties so that we can distinguish between them
    framework.addInstance(Table.class.getName(), table1ID, DatasetProperties.builder().add("tag", "table1").build());
    framework.addInstance(Table.class.getName(), table2ID, DatasetProperties.builder().add("tag", "table2").build());

    // perform some data operations to make sure they are not the same underlying table
    final Table table1 = framework.getDataset(table1ID, Maps.<String, String>newHashMap(), null);
    final Table table2 = framework.getDataset(table2ID, Maps.<String, String>newHashMap(), null);
    TransactionExecutor txnl = new DefaultTransactionExecutor(new MinimalTxSystemClient(),
                                                              (TransactionAware) table1,
                                                              (TransactionAware) table2);
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        table1.put(Bytes.toBytes("rowkey"), Bytes.toBytes("column"), Bytes.toBytes("val1"));
        table2.put(Bytes.toBytes("rowkey"), Bytes.toBytes("column"), Bytes.toBytes("val2"));
      }
    });
    // check data is different, which means they are different underlying tables
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals("val1", Bytes.toString(table1.get(Bytes.toBytes("rowkey"), Bytes.toBytes("column"))));
        Assert.assertEquals("val2", Bytes.toString(table2.get(Bytes.toBytes("rowkey"), Bytes.toBytes("column"))));
      }
    });

    // check get all in a namespace only includes those in that namespace
    Collection<DatasetSpecification> specs = framework.getInstances(namespace1);
    Assert.assertEquals(1, specs.size());
    Assert.assertEquals("table1", specs.iterator().next().getProperty("tag"));
    specs = framework.getInstances(namespace2);
    Assert.assertEquals(1, specs.size());
    Assert.assertEquals("table2", specs.iterator().next().getProperty("tag"));

    // delete one instance and make sure the other still exists
    framework.deleteInstance(table1ID);
    Assert.assertFalse(framework.hasInstance(table1ID));
    Assert.assertTrue(framework.hasInstance(table2ID));

    // delete all instances in one namespace and make sure the other still exists
    framework.addInstance(Table.class.getName(), table1ID, DatasetProperties.EMPTY);
    framework.deleteAllInstances(namespace1);
    Assert.assertTrue(framework.hasInstance(table2ID));

    // delete one namespace and make sure the other still exists
    framework.deleteNamespace(namespace1);
    Assert.assertTrue(framework.hasInstance(table2ID));
  }

  @Test
  public void testNamespaceModuleIsolation() throws Exception {
    DatasetFramework framework = getFramework();

    // create 2 namespaces
    Id.Namespace namespace1 = Id.Namespace.from("ns1");
    Id.Namespace namespace2 = Id.Namespace.from("ns2");
    framework.createNamespace(namespace1);
    framework.createNamespace(namespace2);

    // add modules in each namespace, with one module that shares the same name
    Id.DatasetModule simpleModuleNs1 = Id.DatasetModule.from(namespace1, SimpleKVTable.class.getName());
    Id.DatasetModule simpleModuleNs2 = Id.DatasetModule.from(namespace2, SimpleKVTable.class.getName());
    Id.DatasetModule doubleModuleNs2 = Id.DatasetModule.from(namespace2, DoubleWrappedKVTable.class.getName());
    DatasetModule module1 = new SingleTypeModule(SimpleKVTable.class);
    DatasetModule module2 = new SingleTypeModule(DoubleWrappedKVTable.class);
    framework.addModule(simpleModuleNs1, module1);
    framework.addModule(simpleModuleNs2, module1);
    framework.addModule(doubleModuleNs2, module2);

    // check that we can add instances of datasets in those modules
    framework.addInstance(SimpleKVTable.class.getName(),
                          Id.DatasetInstance.from(namespace1, "kv1"), DatasetProperties.EMPTY);
    framework.addInstance(SimpleKVTable.class.getName(),
                          Id.DatasetInstance.from(namespace2, "kv1"), DatasetProperties.EMPTY);

    // check that only namespace2 can add an instance of this type, since the module should only be in namespace2
    framework.addInstance(DoubleWrappedKVTable.class.getName(),
                          Id.DatasetInstance.from(namespace2, "kv2"), DatasetProperties.EMPTY);
    try {
      framework.addInstance(DoubleWrappedKVTable.class.getName(),
                            Id.DatasetInstance.from(namespace2, "kv2"), DatasetProperties.EMPTY);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }

    // check that deleting all modules from namespace2 does not affect namespace1
    framework.deleteAllInstances(namespace2);
    framework.deleteAllModules(namespace2);
    // should still be able to add an instance in namespace1
    framework.addInstance(SimpleKVTable.class.getName(),
                          Id.DatasetInstance.from(namespace1, "kv3"), DatasetProperties.EMPTY);
    // but not in namespace2
    try {
      framework.addInstance(SimpleKVTable.class.getName(),
                            Id.DatasetInstance.from(namespace2, "kv3"), DatasetProperties.EMPTY);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }

    // add back modules to namespace2
    framework.addModule(simpleModuleNs2, module1);
    framework.addModule(doubleModuleNs2, module2);
    // check that deleting a single module from namespace1 does not affect namespace2
    framework.deleteAllInstances(namespace1);
    framework.deleteModule(simpleModuleNs1);
    // should still be able to add an instance in namespace2
    framework.addInstance(DoubleWrappedKVTable.class.getName(),
                          Id.DatasetInstance.from(namespace2, "kv1"), DatasetProperties.EMPTY);
    // but not in namespace1
    try {
      framework.addInstance(SimpleKVTable.class.getName(),
                            Id.DatasetInstance.from(namespace1, "kv1"), DatasetProperties.EMPTY);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
  }
}
