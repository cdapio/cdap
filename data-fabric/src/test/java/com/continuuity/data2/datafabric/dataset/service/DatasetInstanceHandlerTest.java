package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDatasetDefinition;
import com.continuuity.api.dataset.lib.CompositeDatasetAdmin;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.api.dataset.table.Get;
import com.continuuity.api.dataset.table.Put;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.common.http.HttpRequests;
import com.continuuity.common.http.ObjectResponse;
import com.continuuity.data2.datafabric.dataset.type.DatasetModuleMeta;
import com.continuuity.data2.dataset2.lib.table.CoreDatasetsModule;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryOrderedTableModule;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Unit-test for {@link com.continuuity.data2.datafabric.dataset.service.DatasetInstanceHandler}
 */
public class DatasetInstanceHandlerTest extends DatasetServiceTestBase {

  @Test
  public void testBasics() throws Exception {

    // nothing has been created, modules and types list is empty
    List<DatasetSpecification> instances = getInstances().getResponseObject();

    // nothing in the beginning
    Assert.assertEquals(0, instances.size());

    // create dataset instance with type that is not yet known to the system should fail
    DatasetProperties props = DatasetProperties.builder().add("prop1", "val1").build();
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, createInstance("dataset1", "datasetType2", props));

    // deploy modules
    deployModule("module1", TestModule1.class);
    deployModule("module2", TestModule2.class);

    // create dataset instance
    Assert.assertEquals(HttpStatus.SC_OK, createInstance("dataset1", "datasetType2", props));

    // verify module cannot be deleted which type is used for the dataset
    int modulesBeforeDelete = getModules().getResponseObject().size();
    Assert.assertEquals(HttpStatus.SC_CONFLICT, deleteModule("module2"));
    Assert.assertEquals(HttpStatus.SC_CONFLICT, deleteModules());
    Assert.assertEquals(modulesBeforeDelete, getModules().getResponseObject().size());

    // verify instance was created
    instances = getInstances().getResponseObject();
    Assert.assertEquals(1, instances.size());
    // verifying spec is same as expected
    DatasetSpecification dataset1Spec = createSpec("dataset1", "datasetType2", props);
    Assert.assertEquals(dataset1Spec, instances.get(0));

    // verify created instance info can be retrieved
    DatasetInstanceMeta datasetInfo = getInstance("dataset1").getResponseObject();
    Assert.assertEquals(dataset1Spec, datasetInfo.getSpec());
    Assert.assertEquals(dataset1Spec.getType(), datasetInfo.getType().getName());
    // type meta should have 2 modules that has to be loaded to create type's class and in the order they must be loaded
    List<DatasetModuleMeta> modules = datasetInfo.getType().getModules();
    Assert.assertEquals(2, modules.size());
    DatasetTypeHandlerTest.verify(modules.get(0), "module1", TestModule1.class, ImmutableList.of("datasetType1"),
                                  Collections.<String>emptyList(), ImmutableList.of("module2"));
    DatasetTypeHandlerTest.verify(modules.get(1), "module2", TestModule2.class, ImmutableList.of("datasetType2"),
                                  ImmutableList.of("module1"), Collections.<String>emptyList());

    // try to retrieve non-existed instance
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, getInstance("non-existing-dataset").getResponseCode());

    // cannot create instance with same name again
    Assert.assertEquals(HttpStatus.SC_CONFLICT, createInstance("dataset1", "datasetType2", props));
    Assert.assertEquals(1, getInstances().getResponseObject().size());

    // cannot delete non-existing dataset instance
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, deleteInstance("non-existing-dataset"));
    Assert.assertEquals(1, getInstances().getResponseObject().size());

    // delete dataset instance
    Assert.assertEquals(HttpStatus.SC_OK, deleteInstance("dataset1"));
    Assert.assertEquals(0, getInstances().getResponseObject().size());

    // delete dataset modules
    Assert.assertEquals(HttpStatus.SC_OK, deleteModule("module2"));
    Assert.assertEquals(HttpStatus.SC_OK, deleteModule("module1"));
  }

  @Test
  public void testCreateDelete() throws Exception {
    deployModule("default-orderedTable", InMemoryOrderedTableModule.class);
    deployModule("default-core", CoreDatasetsModule.class);

    // cannot create instance with same name again
    Assert.assertEquals(HttpStatus.SC_OK, createInstance("myTable1", "table", DatasetProperties.EMPTY));
    Assert.assertEquals(HttpStatus.SC_OK, createInstance("myTable2", "table", DatasetProperties.EMPTY));
    Assert.assertEquals(2, getInstances().getResponseObject().size());

    // we want to verify that data is also gone, so we write smth to tables first
    final Table table1 = dsFramework.getDataset("myTable1", null);
    final Table table2 = dsFramework.getDataset("myTable2", null);
    TransactionExecutor txExecutor =
      new DefaultTransactionExecutor(new InMemoryTxSystemClient(txManager),
                                     ImmutableList.of((TransactionAware) table1, (TransactionAware) table2));

    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        table1.put(new Put("key1", "col1", "val1"));
        table2.put(new Put("key2", "col2", "val2"));
      }
    });

    // verify that we can read the data
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals("val1", table1.get(new Get("key1", "col1")).getString("col1"));
        Assert.assertEquals("val2", table2.get(new Get("key2", "col2")).getString("col2"));
      }
    });

    // delete table, check that it is deleted, create again and verify that it is empty
    Assert.assertEquals(HttpStatus.SC_OK, deleteInstance("myTable1"));
    ObjectResponse<List<DatasetSpecification>> instances = getInstances();
    Assert.assertEquals(1, instances.getResponseObject().size());
    Assert.assertEquals("myTable2", instances.getResponseObject().get(0).getName());
    Assert.assertEquals(HttpStatus.SC_OK, createInstance("myTable1", "table", DatasetProperties.EMPTY));
    Assert.assertEquals(2, getInstances().getResponseObject().size());

    // verify that table1 is empty. Note: it is ok for test purpose to re-use the table clients
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertTrue(table1.get(new Get("key1", "col1")).isEmpty());
        Assert.assertEquals("val2", table2.get(new Get("key2", "col2")).getString("col2"));
        // writing smth to table1 for subsequent test
        table1.put(new Put("key3", "col3", "val3"));
      }
    });

    // delete all tables, check that they deleted, create again and verify that they are empty
    Assert.assertEquals(HttpStatus.SC_OK, deleteInstances());
    Assert.assertEquals(0, getInstances().getResponseObject().size());
    Assert.assertEquals(HttpStatus.SC_OK, createInstance("myTable1", "table", DatasetProperties.EMPTY));
    Assert.assertEquals(HttpStatus.SC_OK, createInstance("myTable2", "table", DatasetProperties.EMPTY));
    Assert.assertEquals(2, getInstances().getResponseObject().size());

    // verify that tables are empty. Note: it is ok for test purpose to re-use the table clients
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertTrue(table1.get(new Get("key3", "col3")).isEmpty());
        Assert.assertTrue(table2.get(new Get("key2", "col2")).isEmpty());
      }
    });

    // cleanup
    Assert.assertEquals(HttpStatus.SC_OK, deleteInstances());
    Assert.assertEquals(HttpStatus.SC_OK, deleteModules());
  }

  private int createInstance(String instanceName, String typeName, DatasetProperties props) throws IOException {
    DatasetInstanceHandler.DatasetTypeAndProperties typeAndProps =
      new DatasetInstanceHandler.DatasetTypeAndProperties(typeName, props.getProperties());
    return HttpRequests.put(getUrl("/data/datasets/" + instanceName), new Gson().toJson(typeAndProps))
      .getResponseCode();
  }

  private ObjectResponse<List<DatasetSpecification>> getInstances() throws IOException {
    return ObjectResponse.fromJsonBody(HttpRequests.get(getUrl("/data/datasets")),
                                       new TypeToken<List<DatasetSpecification>>() {
                                       }.getType());
  }

  private ObjectResponse<DatasetInstanceMeta> getInstance(String instanceName) throws IOException {
    return ObjectResponse.fromJsonBody(HttpRequests.get(getUrl("/data/datasets/" + instanceName)),
                                       DatasetInstanceMeta.class);
  }

  private int deleteInstance(String instanceName) throws IOException {
    return HttpRequests.delete(getUrl("/data/datasets/" + instanceName)).getResponseCode();
  }

  /**
   * Test dataset module
   */
  public static class TestModule1 implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      registry.add(createDefinition("datasetType1"));
    }
  }

  /**
   * Test dataset module
   */
  // NOTE: this depends on TestModule
  public static class TestModule2 implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      registry.get("datasetType1");
      registry.add(createDefinition("datasetType2"));
    }
  }

  private static DatasetDefinition createDefinition(String name) {
    return new AbstractDatasetDefinition(name) {
      @Override
      public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
        return createSpec(instanceName, getName(), properties);
      }

      @Override
      public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) {
        return new CompositeDatasetAdmin(Collections.<DatasetAdmin>emptyList());
      }

      @Override
      public Dataset getDataset(DatasetSpecification spec, ClassLoader classLoader) {
        return null;
      }
    };
  }

  private static DatasetSpecification createSpec(String instanceName, String typeName,
                                                DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, typeName).properties(properties.getProperties()).build();
  }
}
