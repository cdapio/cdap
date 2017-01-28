/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.InstanceConflictException;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.kerberos.DefaultOwnerAdmin;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.common.kerberos.OwnerStore;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.test.TestRunner;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.data2.audit.InMemoryAuditPublisher;
import co.cask.cdap.data2.dataset2.lib.file.FileSetModule;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetModule;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework;
import co.cask.cdap.data2.metadata.writer.NoOpLineageWriter;
import co.cask.cdap.data2.registry.NoOpUsageRegistry;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.access.AccessPayload;
import co.cask.cdap.proto.audit.payload.access.AccessType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.security.auth.context.AuthenticationTestContext;
import co.cask.cdap.security.spi.authorization.NoOpAuthorizer;
import co.cask.cdap.store.InMemoryOwnerStore;
import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.tephra.DefaultTransactionExecutor;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.inmemory.MinimalTxSystemClient;
import org.apache.tephra.runtime.TransactionInMemoryModule;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
@RunWith(TestRunner.class)
public abstract class AbstractDatasetFrameworkTest {

  protected abstract DatasetFramework getFramework() throws DatasetManagementException;

  protected static final Map<String, DatasetModule> DEFAULT_MODULES;
  static {
    DEFAULT_MODULES = Maps.newLinkedHashMap();
    DEFAULT_MODULES.put("orderedTable-memory", new InMemoryTableModule());
    DEFAULT_MODULES.put("core", new CoreDatasetsModule());
  }

  protected static final NamespaceId NAMESPACE_ID = new NamespaceId("myspace");
  private static final DatasetModuleId IN_MEMORY = NAMESPACE_ID.datasetModule("inMemory");
  private static final DatasetModuleId CORE = NAMESPACE_ID.datasetModule("core");
  private static final DatasetModuleId FILE = NAMESPACE_ID.datasetModule("file");
  private static final DatasetModuleId PFS = NAMESPACE_ID.datasetModule("pfs");
  private static final DatasetModuleId KEY_VALUE = NAMESPACE_ID.datasetModule("keyValue");
  private static final DatasetModuleId TWICE = NAMESPACE_ID.datasetModule("embedTwice");
  private static final DatasetModuleId DOUBLE_KV = NAMESPACE_ID.datasetModule("doubleKeyValue");
  private static final DatasetId MY_TABLE = NAMESPACE_ID.dataset("my_table");
  private static final DatasetId MY_TABLE2 = NAMESPACE_ID.dataset("my_table2");
  private static final DatasetId MY_DS = NAMESPACE_ID.dataset("myds");
  private static final DatasetTypeId IN_MEMORY_TYPE = NAMESPACE_ID.datasetType("table");
  private static final DatasetTypeId SIMPLE_KV_TYPE = NAMESPACE_ID.datasetType(SimpleKVTable.class.getName());
  private static final DatasetTypeId DOUBLE_KV_TYPE = NAMESPACE_ID.datasetType(DoubleWrappedKVTable.class.getName());
  protected static NamespaceAdmin namespaceAdmin;
  protected static NamespaceQueryAdmin namespaceQueryAdmin;
  protected static OwnerAdmin ownerAdmin;
  protected static DatasetDefinitionRegistryFactory registryFactory;
  protected static CConfiguration cConf;
  protected static TransactionExecutorFactory txExecutorFactory;
  protected static InMemoryAuditPublisher inMemoryAuditPublisher;

  protected static LocationFactory locationFactory;
  protected static NamespacedLocationFactory namespacedLocationFactory;

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws Exception {
    cConf = CConfiguration.create();
    File dataDir = new File(TMP_FOLDER.newFolder(), "data");
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, dataDir.getAbsolutePath());

    final Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new NonCustomLocationUnitTestModule().getModule(),
      new TransactionInMemoryModule(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new AuditModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(OwnerStore.class).to(InMemoryOwnerStore.class).in(Scopes.SINGLETON);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
        }
      }
    );

    locationFactory = injector.getInstance(LocationFactory.class);
    namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);

    txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);
    registryFactory = new DatasetDefinitionRegistryFactory() {
      @Override
      public DatasetDefinitionRegistry create() {
        DefaultDatasetDefinitionRegistry registry = new DefaultDatasetDefinitionRegistry();
        injector.injectMembers(registry);
        return registry;
      }
    };
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    namespaceQueryAdmin = injector.getInstance(NamespaceQueryAdmin.class);
    ownerAdmin = injector.getInstance(OwnerAdmin.class);
    inMemoryAuditPublisher = injector.getInstance(InMemoryAuditPublisher.class);
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(NAMESPACE_ID).build());
  }

  @Test
  public void testSimpleDataset() throws Exception {
    // Configuring Dataset types
    DatasetFramework framework = getFramework();
    // system namespace has a module orderedTable-inMemory
    Assert.assertTrue(framework.hasSystemType("table"));
    // myspace namespace has no modules
    Assert.assertFalse(framework.hasType(IN_MEMORY_TYPE));
    Assert.assertFalse(framework.hasType(SIMPLE_KV_TYPE));
    // add module to namespace 'myspace'
    framework.addModule(KEY_VALUE, new SingleTypeModule(SimpleKVTable.class));
    // make sure it got added to 'myspace'
    Assert.assertTrue(framework.hasType(SIMPLE_KV_TYPE));
    // but not to 'system'
    Assert.assertFalse(framework.hasSystemType(SimpleKVTable.class.getName()));

    Assert.assertFalse(framework.hasInstance(MY_TABLE));
    // Creating instance using a type from own namespace
    framework.addInstance(SimpleKVTable.class.getName(), MY_TABLE, DatasetProperties.EMPTY);
    // verify it got added to the right namespace
    Assert.assertTrue(framework.hasInstance(MY_TABLE));
    // and not to the system namespace
    Assert.assertFalse(framework.hasInstance(NamespaceId.SYSTEM.dataset("my_table")));

    // Doing some admin and data ops
    DatasetAdmin admin = framework.getAdmin(MY_TABLE, null);
    Assert.assertNotNull(admin);
    final SimpleKVTable kvTable = framework.getDataset(MY_TABLE, DatasetDefinition.NO_ARGUMENTS, null);
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
    framework.deleteInstance(MY_TABLE);
    framework.deleteModule(KEY_VALUE);

    // recreate instance without adding a module in 'myspace'. This should use types from default namespace
    framework.addInstance("table", MY_TABLE, DatasetProperties.EMPTY);
    // verify it got added to the right namespace
    Assert.assertTrue(framework.hasInstance(MY_TABLE));
    admin = framework.getAdmin(MY_TABLE, null);
    Assert.assertNotNull(admin);
    final Table table = framework.getDataset(MY_TABLE, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(table);
    txnl = new DefaultTransactionExecutor(new MinimalTxSystemClient(), (TransactionAware) table);
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
    framework.deleteInstance(MY_TABLE);
  }

  @Test
  public void testCompositeDataset() throws Exception {
    // Configuring Dataset types
    DatasetFramework framework = getFramework();

    framework.addModule(IN_MEMORY, new InMemoryTableModule());
    framework.addModule(CORE, new CoreDatasetsModule());
    Assert.assertFalse(framework.hasSystemType(SimpleKVTable.class.getName()));
    Assert.assertFalse(framework.hasType(SIMPLE_KV_TYPE));
    framework.addModule(KEY_VALUE, new SingleTypeModule(SimpleKVTable.class));
    Assert.assertTrue(framework.hasType(SIMPLE_KV_TYPE));
    Assert.assertFalse(framework.hasSystemType(SimpleKVTable.class.getName()));

    // Creating instance
    Assert.assertFalse(framework.hasInstance(MY_TABLE));
    framework.addInstance(SimpleKVTable.class.getName(), MY_TABLE, DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance(MY_TABLE));

    testCompositeDataset(framework);

    // cleanup
    framework.deleteInstance(MY_TABLE);
    framework.deleteModule(KEY_VALUE);
    framework.deleteModule(CORE);
    framework.deleteModule(IN_MEMORY);
  }

  @Test
  public void testDoubleCompositeDataset() throws Exception {
    // Configuring Dataset types
    DatasetFramework framework = getFramework();

    framework.addModule(IN_MEMORY, new InMemoryTableModule());
    framework.addModule(CORE, new CoreDatasetsModule());
    framework.addModule(KEY_VALUE, new SingleTypeModule(SimpleKVTable.class));
    Assert.assertFalse(framework.hasSystemType(DoubleWrappedKVTable.class.getName()));
    Assert.assertFalse(framework.hasType(DOUBLE_KV_TYPE));
    framework.addModule(DOUBLE_KV, new SingleTypeModule(DoubleWrappedKVTable.class));
    Assert.assertTrue(framework.hasType(DOUBLE_KV_TYPE));
    Assert.assertFalse(framework.hasSystemType(DoubleWrappedKVTable.class.getName()));

    // Creating instance
    Assert.assertFalse(framework.hasInstance(MY_TABLE));
    framework.addInstance(DoubleWrappedKVTable.class.getName(), MY_TABLE, DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance(MY_TABLE));

    testCompositeDataset(framework);

    // cleanup
    framework.deleteInstance(MY_TABLE);
    framework.deleteModule(DOUBLE_KV);
    framework.deleteModule(KEY_VALUE);
    framework.deleteModule(CORE);
    framework.deleteModule(IN_MEMORY);
  }

  private void testCompositeDataset(DatasetFramework framework) throws Exception {

    // Doing some admin and data ops
    DatasetAdmin admin = framework.getAdmin(MY_TABLE, null);
    Assert.assertNotNull(admin);
    final KeyValueTable table = framework.getDataset(MY_TABLE, DatasetDefinition.NO_ARGUMENTS, null);
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
  public void testMultipleTransitiveDependencies() throws DatasetManagementException, IOException {
    // Adding modules
    DatasetFramework framework = getFramework();
    try {
      framework.addModule(IN_MEMORY, new InMemoryTableModule());
      framework.addModule(CORE, new CoreDatasetsModule());
      framework.addModule(FILE, new FileSetModule());
      framework.addModule(PFS, new PartitionedFileSetModule());
      framework.addModule(TWICE, new SingleTypeModule(EmbedsTableTwiceDataset.class));

      // Creating an instances
      framework.addInstance(EmbedsTableTwiceDataset.class.getName(), MY_DS,
                            PartitionedFileSetProperties.builder()
                              .setPartitioning(Partitioning.builder().addStringField("x").build())
                              .build());
      Assert.assertTrue(framework.hasInstance(MY_DS));
      framework.getDataset(MY_DS, DatasetProperties.EMPTY.getProperties(), null);
    } finally {
      framework.deleteAllInstances(NAMESPACE_ID);
      framework.deleteAllModules(NAMESPACE_ID);
    }
  }

  @Test
  public void testBasicManagement() throws Exception {
    DatasetTypeId tableType = NAMESPACE_ID.datasetType(Table.class.getName());

    // Adding modules
    DatasetFramework framework = getFramework();

    framework.addModule(IN_MEMORY, new InMemoryTableModule());
    framework.addModule(CORE, new CoreDatasetsModule());
    framework.addModule(FILE, new FileSetModule());
    framework.addModule(KEY_VALUE, new SingleTypeModule(SimpleKVTable.class));
    // keyvalue has been added in the system namespace
    Assert.assertTrue(framework.hasSystemType(Table.class.getName()));
    Assert.assertFalse(framework.hasSystemType(SimpleKVTable.class.getName()));
    Assert.assertTrue(framework.hasType(tableType));
    Assert.assertTrue(framework.hasType(SIMPLE_KV_TYPE));

    // Creating instances
    framework.addInstance(Table.class.getName(), MY_TABLE, DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance(MY_TABLE));
    DatasetSpecification spec = framework.getDatasetSpec(MY_TABLE);
    Assert.assertNotNull(spec);
    Assert.assertEquals(MY_TABLE.getEntityName(), spec.getName());
    Assert.assertEquals(Table.class.getName(), spec.getType());
    framework.addInstance(Table.class.getName(), MY_TABLE2, DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance(MY_TABLE2));

    // Update instances
    File baseDir = TMP_FOLDER.newFolder();
    framework.addInstance(FileSet.class.getName(), MY_DS, FileSetProperties.builder()
      .setBasePath(baseDir.getPath())
      .setDataExternal(true)
      .build());
    // this should fail because it would "internalize" external data
    try {
      framework.updateInstance(MY_DS, DatasetProperties.EMPTY);
      Assert.fail("update should have thrown instance conflict");
    } catch (InstanceConflictException e) {
      // expected
    }
    baseDir = TMP_FOLDER.newFolder();
    // this should succeed because it simply changes the external path
    framework.updateInstance(MY_DS, FileSetProperties.builder()
      .setBasePath(baseDir.getPath())
      .setDataExternal(true)
      .build());
    spec = framework.getDatasetSpec(MY_DS);
    Assert.assertNotNull(spec);
    Assert.assertEquals(baseDir.getPath(), FileSetProperties.getBasePath(spec.getProperties()));

    // cleanup
    try {
      framework.deleteAllModules(NAMESPACE_ID);
      Assert.fail("should not delete modules: there are datasets using their types");
    } catch (DatasetManagementException e) {
      // expected
    }
    // types are still there
    Assert.assertTrue(framework.hasType(tableType));
    Assert.assertTrue(framework.hasType(SIMPLE_KV_TYPE));

    framework.deleteAllInstances(NAMESPACE_ID);
    Assert.assertEquals(0, framework.getInstances(NAMESPACE_ID).size());
    Assert.assertFalse(framework.hasInstance(MY_TABLE));
    Assert.assertNull(framework.getDatasetSpec(MY_TABLE));
    Assert.assertFalse(framework.hasInstance(MY_TABLE2));
    Assert.assertNull(framework.getDatasetSpec(MY_TABLE2));

    // now it should succeed
    framework.deleteAllModules(NAMESPACE_ID);
    Assert.assertTrue(framework.hasSystemType(Table.class.getName()));
    Assert.assertFalse(framework.hasType(tableType));
    Assert.assertFalse(framework.hasType(SIMPLE_KV_TYPE));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testNamespaceInstanceIsolation() throws Exception {
    DatasetFramework framework = getFramework();

    // create 2 namespaces
    NamespaceId namespace1 = new NamespaceId("ns1");
    NamespaceId namespace2 = new NamespaceId("ns2");
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(namespace1).build());
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(namespace2).build());
    namespacedLocationFactory.get(namespace1.toId()).mkdirs();
    namespacedLocationFactory.get(namespace2.toId()).mkdirs();

    // create 2 tables, one in each namespace. both tables have the same name.
    DatasetId table1ID = namespace1.dataset("table");
    DatasetId table2ID = namespace2.dataset("table");
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
    Collection<DatasetSpecificationSummary> specs = framework.getInstances(namespace1);
    Assert.assertEquals(1, specs.size());
    Assert.assertEquals("table1", specs.iterator().next().getProperties().get("tag"));
    specs = framework.getInstances(namespace2);
    Assert.assertEquals(1, specs.size());
    Assert.assertEquals("table2", specs.iterator().next().getProperties().get("tag"));

    // delete one instance and make sure the other still exists
    framework.deleteInstance(table1ID);
    Assert.assertFalse(framework.hasInstance(table1ID));
    Assert.assertTrue(framework.hasInstance(table2ID));

    // delete all instances in one namespace and make sure the other still exists
    framework.addInstance(Table.class.getName(), table1ID, DatasetProperties.EMPTY);
    framework.deleteAllInstances(namespace1);
    Assert.assertTrue(framework.hasInstance(table2ID));

    // delete one namespace and make sure the other still exists
    namespacedLocationFactory.get(namespace1.toId()).delete(true);
    Assert.assertTrue(framework.hasInstance(table2ID));
  }

  @Test
  public void testNamespaceModuleIsolation() throws Exception {
    DatasetFramework framework = getFramework();

    // create 2 namespaces
    NamespaceId namespace1 = new NamespaceId("ns1");
    NamespaceId namespace2 = new NamespaceId("ns2");
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(namespace1).build());
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(namespace2).build());
    namespacedLocationFactory.get(namespace1.toId()).mkdirs();
    namespacedLocationFactory.get(namespace2.toId()).mkdirs();

    // add modules in each namespace, with one module that shares the same name
    DatasetModuleId simpleModuleNs1 = namespace1.datasetModule(SimpleKVTable.class.getName());
    DatasetModuleId simpleModuleNs2 = namespace2.datasetModule(SimpleKVTable.class.getName());
    DatasetModuleId doubleModuleNs2 = namespace2.datasetModule(DoubleWrappedKVTable.class.getName());
    DatasetModule module1 = new SingleTypeModule(SimpleKVTable.class);
    DatasetModule module2 = new SingleTypeModule(DoubleWrappedKVTable.class);
    framework.addModule(simpleModuleNs1, module1);
    framework.addModule(simpleModuleNs2, module1);
    framework.addModule(doubleModuleNs2, module2);

    // check that we can add instances of datasets in those modules
    framework.addInstance(SimpleKVTable.class.getName(),
                          namespace1.dataset("kv1"), DatasetProperties.EMPTY);
    framework.addInstance(SimpleKVTable.class.getName(),
                          namespace2.dataset("kv1"), DatasetProperties.EMPTY);

    // check that only namespace2 can add an instance of this type, since the module should only be in namespace2
    framework.addInstance(DoubleWrappedKVTable.class.getName(), 
                          namespace2.dataset("kv2"), DatasetProperties.EMPTY);
    try {
      framework.addInstance(DoubleWrappedKVTable.class.getName(), 
                            namespace2.dataset("kv2"), DatasetProperties.EMPTY);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }

    // check that deleting all modules from namespace2 does not affect namespace1
    framework.deleteAllInstances(namespace2);
    framework.deleteAllModules(namespace2);
    // should still be able to add an instance in namespace1
    framework.addInstance(SimpleKVTable.class.getName(),
                          namespace1.dataset("kv3"), DatasetProperties.EMPTY);
    // but not in namespace2
    try {
      framework.addInstance(SimpleKVTable.class.getName(),
                            namespace2.dataset("kv3"), DatasetProperties.EMPTY);
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
                          namespace2.dataset("kv1"), DatasetProperties.EMPTY);
    // but not in namespace1
    try {
      framework.addInstance(SimpleKVTable.class.getName(),
                            namespace1.dataset("kv1"), DatasetProperties.EMPTY);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testAuditPublish() throws Exception {
    // Clear all audit messages
    inMemoryAuditPublisher.popMessages();
    List<AuditMessage> expectedMessages = new ArrayList<>();

    // Adding modules
    DatasetFramework framework = getFramework();
    framework.addModule(IN_MEMORY, new InMemoryTableModule());

    // Creating instances
    framework.addInstance(Table.class.getName(), MY_TABLE, DatasetProperties.EMPTY);
    expectedMessages.add(new AuditMessage(0, MY_TABLE, "", AuditType.CREATE, AuditPayload.EMPTY_PAYLOAD));
    framework.addInstance(Table.class.getName(), MY_TABLE2, DatasetProperties.EMPTY);
    expectedMessages.add(new AuditMessage(0, MY_TABLE2, "", AuditType.CREATE, AuditPayload.EMPTY_PAYLOAD));

    // Update instance
    framework.updateInstance(MY_TABLE, DatasetProperties.EMPTY);
    expectedMessages.add(new AuditMessage(0, MY_TABLE, "", AuditType.UPDATE, AuditPayload.EMPTY_PAYLOAD));

    // Access instance
    ProgramRunId runId = new ProgramId("ns", "app", ProgramType.FLOW, "flow").run(RunIds.generate().getId());
    LineageWriterDatasetFramework lineageFramework =
      new LineageWriterDatasetFramework(framework, new NoOpLineageWriter(), new NoOpUsageRegistry(),
                                        new AuthenticationTestContext(), new NoOpAuthorizer());
    lineageFramework.initContext(runId);
    lineageFramework.setAuditPublisher(inMemoryAuditPublisher);
    lineageFramework.getDataset(MY_TABLE, null, getClass().getClassLoader());
    expectedMessages.add(new AuditMessage(0, MY_TABLE, "", AuditType.ACCESS,
                                          new AccessPayload(AccessType.UNKNOWN, runId)));

    // Truncate instance
    framework.truncateInstance(MY_TABLE);
    expectedMessages.add(new AuditMessage(0, MY_TABLE, "", AuditType.TRUNCATE,
                                          AuditPayload.EMPTY_PAYLOAD));

    // Delete instance
    framework.deleteInstance(MY_TABLE);
    expectedMessages.add(new AuditMessage(0, MY_TABLE, "", AuditType.DELETE, AuditPayload.EMPTY_PAYLOAD));

    // Delete all instances in a namespace
    framework.deleteAllInstances(MY_TABLE2.getParent());
    expectedMessages.add(new AuditMessage(0, MY_TABLE2, "", AuditType.DELETE, AuditPayload.EMPTY_PAYLOAD));

    Assert.assertEquals(expectedMessages, inMemoryAuditPublisher.popMessages());

    // cleanup
    framework.deleteModule(IN_MEMORY);
  }
}
