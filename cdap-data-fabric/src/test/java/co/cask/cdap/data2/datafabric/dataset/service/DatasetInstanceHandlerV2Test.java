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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.Id;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.ObjectResponse;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
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
 * Unit-test for {@link DatasetInstanceHandlerV2}
 */
public class DatasetInstanceHandlerV2Test extends DatasetServiceTestBase {

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
    DatasetTypeHandlerV2Test.verify(modules.get(0), "module1", TestModule1.class, ImmutableList.of("datasetType1"),
                                    Collections.<String>emptyList(), ImmutableList.of("module2"));
    DatasetTypeHandlerV2Test.verify(modules.get(1), "module2", TestModule2.class, ImmutableList.of("datasetType2"),
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
  public void testUpdateInstance() throws Exception {

    // nothing has been created, modules and types list is empty
    List<DatasetSpecification> instances = getInstances().getResponseObject();

    // nothing in the beginning
    Assert.assertEquals(0, instances.size());

    DatasetProperties props = DatasetProperties.builder().add("prop1", "val1").build();

    // deploy modules
    deployModule("module1", TestModule1.class);
    deployModule("module2", TestModule2.class);

    // create dataset instance
    Assert.assertEquals(HttpStatus.SC_OK, createInstance("dataset1", "datasetType2", props));

    // verify instance was created
    instances = getInstances().getResponseObject();
    Assert.assertEquals(1, instances.size());

    DatasetProperties newProps = DatasetProperties.builder().add("prop2", "val2").build();

    // update dataset instance
    Assert.assertEquals(HttpStatus.SC_OK, updateInstance("dataset1", "datasetType2", newProps));
    Assert.assertEquals("val2", getInstance("dataset1").getResponseObject().getSpec().getProperty("prop2"));
    Assert.assertNull(getInstance("dataset1").getResponseObject().getSpec().getProperty("prop1"));

    // delete dataset instance
    Assert.assertEquals(HttpStatus.SC_OK, deleteInstance("dataset1"));
    Assert.assertEquals(0, getInstances().getResponseObject().size());

    // delete dataset modules
    Assert.assertEquals(HttpStatus.SC_OK, deleteModule("module2"));
    Assert.assertEquals(HttpStatus.SC_OK, deleteModule("module1"));
  }

  @Test
  public void testCreateDelete() throws Exception {
    deployModule("default-orderedTable", InMemoryTableModule.class);
    deployModule("default-core", CoreDatasetsModule.class);

    // cannot create instance with same name again
    Assert.assertEquals(HttpStatus.SC_OK, createInstance("myTable1", "table", DatasetProperties.EMPTY));
    Assert.assertEquals(HttpStatus.SC_OK, createInstance("myTable2", "table", DatasetProperties.EMPTY));
    Assert.assertEquals(2, getInstances().getResponseObject().size());

    // we want to verify that data is also gone, so we write smth to tables first
    // using default namespace here since this is testing v2 APIs right now. TODO: add tests for v3 APIs
    final Table table1 = dsFramework.getDataset(Id.DatasetInstance.from(Constants.DEFAULT_NAMESPACE, "myTable1"),
                                                DatasetDefinition.NO_ARGUMENTS, null);
    final Table table2 = dsFramework.getDataset(Id.DatasetInstance.from(Constants.DEFAULT_NAMESPACE, "myTable2"),
                                                DatasetDefinition.NO_ARGUMENTS, null);
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
    deleteInstances();
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
    deleteInstances();
    Assert.assertEquals(HttpStatus.SC_OK, deleteModules());
  }


  private int createInstance(String instanceName, String typeName,
                             DatasetProperties props) throws IOException {
    DatasetInstanceConfiguration creationProperties =
      new DatasetInstanceConfiguration(typeName, props.getProperties());

    HttpRequest request = HttpRequest.put(getUrl("/data/datasets/" + instanceName))
      .withBody(new Gson().toJson(creationProperties)).build();
    return HttpRequests.execute(request).getResponseCode();
  }

  private int updateInstance(String instanceName, String typeName,
                             DatasetProperties props) throws IOException {
    DatasetInstanceConfiguration creationProperties =
      new DatasetInstanceConfiguration(typeName, props.getProperties());

    HttpRequest request = HttpRequest.put(getUrl("/data/datasets/" + instanceName + "/properties"))
      .withBody(new Gson().toJson(creationProperties)).build();
    return HttpRequests.execute(request).getResponseCode();
  }

  private ObjectResponse<List<DatasetSpecification>> getInstances() throws IOException {
    HttpRequest request = HttpRequest.get(getUrl("/data/datasets")).build();
    return ObjectResponse.fromJsonBody(HttpRequests.execute(request),
                                       new TypeToken<List<DatasetSpecification>>() { }.getType());
  }

  private ObjectResponse<DatasetInstanceMeta> getInstance(String instanceName) throws IOException {
    HttpRequest request = HttpRequest.get(getUrl("/data/datasets/" + instanceName)).build();
    return ObjectResponse.fromJsonBody(HttpRequests.execute(request), DatasetInstanceMeta.class);
  }

  private int deleteInstance(String instanceName) throws IOException {
    HttpRequest request = HttpRequest.delete(getUrl("/data/datasets/" + instanceName)).build();
    return HttpRequests.execute(request).getResponseCode();
  }

  private void deleteInstances() throws IOException {
    // NOTE: intentionally, there's no endpoint to delete all datasets instances currently to prevent bad things
    //       happen by accident
    for (DatasetSpecification spec : getInstances().getResponseObject()) {
      deleteInstance(spec.getName());
    }
  }

  private static DatasetSpecification createSpec(String instanceName, String typeName,
                                                 DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, typeName).properties(properties.getProperties()).build();
  }
}
