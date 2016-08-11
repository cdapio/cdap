/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseConsumerStateStore;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseQueueDatasetModule;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
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
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Unit-test for {@link DatasetInstanceHandler}
 */
public class DatasetInstanceHandlerTest extends DatasetServiceTestBase {

  private static final Gson GSON = new Gson();

  @BeforeClass
  public static void setup() throws Exception {
    DatasetServiceTestBase.initialize();
  }

  @Test
  public void testSystemDatasetNotInList() throws Exception {
    try {
      deployModule("default-table", InMemoryTableModule.class);
      deployModule(HBaseConsumerStateStore.class.getSimpleName(), HBaseQueueDatasetModule.class);
      // yes it's weird, you can create one a system dataset, but don't expect to see it in the get all request
      Assert.assertEquals(HttpStatus.SC_OK,
                          createInstance(QueueConstants.STATE_STORE_NAME, HBaseConsumerStateStore.class.getSimpleName())
                            .getResponseCode());

      // nothing has been created, modules and types list is empty
      Assert.assertTrue(getInstances().getResponseObject().isEmpty());
    } finally {
      // cleanup
      deleteInstance(QueueConstants.STATE_STORE_NAME);
      Assert.assertEquals(HttpStatus.SC_OK, deleteModules().getResponseCode());
    }
  }

  @Test
  public void testBasics() throws Exception {
    // nothing has been created, modules and types list is empty
    List<DatasetSpecificationSummary> instances = getInstances().getResponseObject();

    // nothing in the beginning
    Assert.assertEquals(0, instances.size());

    try {
      // create dataset instance with type that is not yet known to the system should fail
      DatasetProperties props = DatasetProperties.builder().add("prop1", "val1").build();
      Assert.assertEquals(HttpStatus.SC_NOT_FOUND, createInstance("dataset1", "datasetType2", props).getResponseCode());

      // deploy modules
      deployModule("module1", TestModule1.class);
      deployModule("module2", TestModule2.class);

      // create dataset instance
      String description = "test instance description";
      HttpResponse response = createInstance("dataset1", "datasetType2", description, props);
      Assert.assertEquals(HttpStatus.SC_OK, response.getResponseCode());

      // verify module cannot be deleted which type is used for the dataset
      int modulesBeforeDelete = getModules().getResponseObject().size();
      Assert.assertEquals(HttpStatus.SC_CONFLICT, deleteModule("module2").getResponseCode());
      Assert.assertEquals(HttpStatus.SC_CONFLICT, deleteModules().getResponseCode());
      Assert.assertEquals(modulesBeforeDelete, getModules().getResponseObject().size());

      // verify instance was created
      instances = getInstances().getResponseObject();
      Assert.assertEquals(1, instances.size());
      // verifying spec is same as expected
      DatasetSpecification dataset1Spec = createType2Spec("dataset1", "datasetType2", description, props);
      Assert.assertEquals(spec2Summary(dataset1Spec), instances.get(0));

      // verify created instance info can be retrieved
      DatasetMeta datasetInfo = getInstanceObject("dataset1").getResponseObject();
      Assert.assertEquals(dataset1Spec, datasetInfo.getSpec());
      Assert.assertEquals(dataset1Spec.getType(), datasetInfo.getType().getName());
      // type meta should have 2 modules that has to be loaded to create type's class
      // and in the order they must be loaded
      List<DatasetModuleMeta> modules = datasetInfo.getType().getModules();
      Assert.assertEquals(2, modules.size());
      DatasetTypeHandlerTest.verify(modules.get(0), "module1", TestModule1.class, ImmutableList.of("datasetType1"),
                                    Collections.<String>emptyList(), ImmutableList.of("module2"));
      DatasetTypeHandlerTest.verify(modules.get(1), "module2", TestModule2.class, ImmutableList.of("datasetType2"),
                                    ImmutableList.of("module1"), Collections.<String>emptyList());

      // try to retrieve non-existed instance
      Assert.assertEquals(HttpStatus.SC_NOT_FOUND, getInstance("non-existing-dataset").getResponseCode());

      // cannot create instance with same name again
      Assert.assertEquals(HttpStatus.SC_CONFLICT, createInstance("dataset1", "datasetType2", props).getResponseCode());
      Assert.assertEquals(1, getInstances().getResponseObject().size());

      // cannot delete non-existing dataset instance
      Assert.assertEquals(HttpStatus.SC_NOT_FOUND, deleteInstance("non-existing-dataset").getResponseCode());
      Assert.assertEquals(1, getInstances().getResponseObject().size());

      // verify creation of dataset instance with null properties
      Assert.assertEquals(HttpStatus.SC_OK, createInstance("nullPropertiesTable", "datasetType2").getResponseCode());
      // since dataset instance description is not provided, we are using the description given by the dataset type
      DatasetSpecification nullPropertiesTableSpec = createType2Spec("nullPropertiesTable", "datasetType2",
                                                          TestModule2.DESCRIPTION, DatasetProperties.EMPTY);
      DatasetSpecificationSummary actualSummary = getSummaryForInstance("nullPropertiesTable",
                                                                        getInstances().getResponseObject());
      Assert.assertEquals(spec2Summary(nullPropertiesTableSpec), actualSummary);

      // delete dataset instance
      Assert.assertEquals(HttpStatus.SC_OK, deleteInstance("dataset1").getResponseCode());
      Assert.assertEquals(HttpStatus.SC_OK, deleteInstance("nullPropertiesTable").getResponseCode());
      Assert.assertEquals(0, getInstances().getResponseObject().size());

      // create workflow local dataset instance
      DatasetProperties localDSProperties = DatasetProperties.builder().add("prop1", "val1")
        .add(Constants.AppFabric.WORKFLOW_LOCAL_DATASET_PROPERTY, "true").build();
      Assert.assertEquals(HttpStatus.SC_OK, createInstance("localDSInstance", "datasetType2",
                                                           localDSProperties).getResponseCode());

      // getInstances call should still return 0
      Assert.assertEquals(0, getInstances().getResponseObject().size());
      Assert.assertEquals(HttpStatus.SC_OK, deleteInstance("localDSInstance").getResponseCode());

      // delete dataset modules
      Assert.assertEquals(HttpStatus.SC_OK, deleteModule("module2").getResponseCode());
      Assert.assertEquals(HttpStatus.SC_OK, deleteModule("module1").getResponseCode());

    } finally {
      deleteInstance("dataset1");
      deleteInstance("nullPropertiesTable");
      deleteInstance("localDSInstance");
      deleteModule("module2");
      deleteModule("module1");
    }
  }

  @Test
  public void testUpdateInstance() throws Exception {
    // nothing has been created, modules and types list is empty
    List<DatasetSpecificationSummary> instances = getInstances().getResponseObject();

    // nothing in the beginning
    Assert.assertEquals(0, instances.size());

    try {
      DatasetProperties props = DatasetProperties.builder()
        .add("prop1", "val1")
        .add(TestModule2.NOT_RECONFIGURABLE, "this")
        .build();

      // deploy modules
      deployModule("module1", TestModule1.class);
      deployModule("module2", TestModule2.class);

      // create dataset instance
      Assert.assertEquals(HttpStatus.SC_OK, createInstance("dataset1", "datasetType2", props).getResponseCode());

      // verify instance was created
      instances = getInstances().getResponseObject();
      Assert.assertEquals(1, instances.size());

      DatasetMeta meta = getInstanceObject("dataset1").getResponseObject();
      Map<String, String> storedOriginalProps = meta.getSpec().getOriginalProperties();
      Assert.assertEquals(props.getProperties(), storedOriginalProps);

      Map<String, String> retrievedProps = getInstanceProperties("dataset1").getResponseObject();
      Assert.assertEquals(props.getProperties(), retrievedProps);

      // these properties are incompatible because TestModule1.NOT_RECONFIGURABLE may not change
      DatasetProperties newProps = DatasetProperties.builder()
        .add("prop2", "val2")
        .add(TestModule2.NOT_RECONFIGURABLE, "that")
        .build();
      Assert.assertEquals(HttpStatus.SC_CONFLICT, updateInstance("dataset1", newProps).getResponseCode());

      // update dataset instance with valid properties
      newProps = DatasetProperties.builder()
        .add("prop2", "val2")
        .add(TestModule2.NOT_RECONFIGURABLE, "this")
        .build();
      Assert.assertEquals(HttpStatus.SC_OK, updateInstance("dataset1", newProps).getResponseCode());

      meta = getInstanceObject("dataset1").getResponseObject();
      Assert.assertEquals(newProps.getProperties(), meta.getSpec().getOriginalProperties());
      Assert.assertEquals("val2", meta.getSpec().getProperty("prop2"));
      Assert.assertNull(meta.getSpec().getProperty("prop1"));

      retrievedProps = getInstanceProperties("dataset1").getResponseObject();
      Assert.assertEquals(newProps.getProperties(), retrievedProps);

    } finally {
      // delete dataset instance
      Assert.assertEquals(HttpStatus.SC_OK, deleteInstance("dataset1").getResponseCode());
      Assert.assertEquals(0, getInstances().getResponseObject().size());

      // delete dataset modules
      Assert.assertEquals(HttpStatus.SC_OK, deleteModule("module2").getResponseCode());
      Assert.assertEquals(HttpStatus.SC_OK, deleteModule("module1").getResponseCode());
    }
  }

  @Test
  public void testCreateDelete() throws Exception {
    try {
      deployModule("default-table", InMemoryTableModule.class);
      deployModule("default-core", CoreDatasetsModule.class);

      // cannot create instance with same name again
      Assert.assertEquals(HttpStatus.SC_OK,
                          createInstance("myTable1", "table", DatasetProperties.EMPTY).getResponseCode());
      Assert.assertEquals(HttpStatus.SC_OK,
                          createInstance("myTable2", "table", DatasetProperties.EMPTY).getResponseCode());
      Assert.assertEquals(2, getInstances().getResponseObject().size());

      // we want to verify that data is also gone, so we write smth to tables first
      final Table table1 = dsFramework.getDataset(Id.DatasetInstance.from(Id.Namespace.DEFAULT, "myTable1"),
                                                  DatasetDefinition.NO_ARGUMENTS, null);
      final Table table2 = dsFramework.getDataset(Id.DatasetInstance.from(Id.Namespace.DEFAULT, "myTable2"),
                                                  DatasetDefinition.NO_ARGUMENTS, null);
      Assert.assertNotNull(table1);
      Assert.assertNotNull(table2);
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
      Assert.assertEquals(HttpStatus.SC_OK, deleteInstance("myTable1").getResponseCode());
      ObjectResponse<List<DatasetSpecificationSummary>> instances = getInstances();
      Assert.assertEquals(1, instances.getResponseObject().size());
      Assert.assertEquals("myTable2", instances.getResponseObject().get(0).getName());
      Assert.assertEquals(HttpStatus.SC_OK,
                          createInstance("myTable1", "table", DatasetProperties.EMPTY).getResponseCode());
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
      Assert.assertEquals(HttpStatus.SC_OK,
                          createInstance("myTable1", "table", DatasetProperties.EMPTY).getResponseCode());
      Assert.assertEquals(HttpStatus.SC_OK,
                          createInstance("myTable2", "table", DatasetProperties.EMPTY).getResponseCode());
      Assert.assertEquals(2, getInstances().getResponseObject().size());

      // verify that tables are empty. Note: it is ok for test purpose to re-use the table clients
      txExecutor.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          Assert.assertTrue(table1.get(new Get("key3", "col3")).isEmpty());
          Assert.assertTrue(table2.get(new Get("key2", "col2")).isEmpty());
        }
      });
    } finally {
      // cleanup
      deleteInstances();
      Assert.assertEquals(HttpStatus.SC_OK, deleteModules().getResponseCode());
    }
  }

  @Test
  public void testNotFound() throws IOException {
    Id.Namespace nonExistent = Id.Namespace.from("nonexistent");
    Id.DatasetInstance datasetInstance = Id.DatasetInstance.from(nonExistent, "ds");

    HttpResponse response = makeInstancesRequest(nonExistent.getId());
    assertNamespaceNotFound(response, nonExistent);

    // TODO: commented out for now until we add back namespace checks on get dataset CDAP-3901
//    response = getInstance(datasetInstance);
//    assertNamespaceNotFound(response, nonExistent);

    response = createInstance(datasetInstance, Table.class.getName(), null);
    assertNamespaceNotFound(response, nonExistent);

    response = updateInstance(datasetInstance, DatasetProperties.EMPTY);
    assertNamespaceNotFound(response, nonExistent);

    response = deleteInstance(datasetInstance);
    assertNamespaceNotFound(response, nonExistent);

    // it should be ok to use system
    response = getInstances(Id.Namespace.SYSTEM.getId());
    Assert.assertEquals(200, response.getResponseCode());
  }

  private HttpResponse createInstance(String instanceName, String typeName,
                                      DatasetProperties props) throws IOException {
    return createInstance(Id.DatasetInstance.from(Id.Namespace.DEFAULT, instanceName), typeName, props);
  }

  private HttpResponse createInstance(String instanceName, String typeName, String description,
                                      DatasetProperties props) throws IOException {
    return createInstance(Id.DatasetInstance.from(Id.Namespace.DEFAULT, instanceName), typeName, description, props);
  }

  private HttpResponse createInstance(String instanceName, String typeName) throws IOException {
    return createInstance(Id.DatasetInstance.from(Id.Namespace.DEFAULT, instanceName), typeName, null);
  }

  private HttpResponse createInstance(Id.DatasetInstance instance, String typeName,
                                      @Nullable DatasetProperties props) throws IOException {
    return createInstance(instance, typeName, null, props);
  }

  private HttpResponse createInstance(Id.DatasetInstance instance, String typeName, @Nullable String description,
                                      @Nullable DatasetProperties props) throws IOException {
    DatasetInstanceConfiguration creationProperties;
    if (props != null) {
      creationProperties = new DatasetInstanceConfiguration(typeName, props.getProperties(), description);
    } else {
      creationProperties = new DatasetInstanceConfiguration(typeName, null, description);
    }
    HttpRequest request = HttpRequest.put(getUrl(instance.getNamespaceId(), "/data/datasets/" + instance.getId()))
      .withBody(GSON.toJson(creationProperties)).build();
    return HttpRequests.execute(request);
  }

  private HttpResponse updateInstance(String instanceName, DatasetProperties props) throws IOException {
    return updateInstance(Id.DatasetInstance.from(Id.Namespace.DEFAULT, instanceName), props);
  }

  private HttpResponse updateInstance(Id.DatasetInstance instance, DatasetProperties props) throws IOException {
    HttpRequest request = HttpRequest.put(getUrl(instance.getNamespaceId(),
                                                 "/data/datasets/" + instance.getId() + "/properties"))
      .withBody(GSON.toJson(props.getProperties())).build();
    return HttpRequests.execute(request);
  }

  private ObjectResponse<List<DatasetSpecificationSummary>> getInstances() throws IOException {
    return getInstances(Id.Namespace.DEFAULT.getId());
  }

  private ObjectResponse<List<DatasetSpecificationSummary>> getInstances(String namespace) throws IOException {
    return ObjectResponse.fromJsonBody(makeInstancesRequest(namespace),
                                       new TypeToken<List<DatasetSpecificationSummary>>() { }.getType());
  }

  private HttpResponse makeInstancesRequest(String namespace) throws IOException {
    HttpRequest request = HttpRequest.get(getUrl(namespace, "/data/datasets")).build();
    return HttpRequests.execute(request);
  }

  private HttpResponse getInstance(String instanceName) throws IOException {
    return getInstance(Id.DatasetInstance.from(Id.Namespace.DEFAULT, instanceName));
  }

  private HttpResponse getInstance(Id.DatasetInstance instance) throws IOException {
    URL instanceUrl = getUrl(instance.getNamespaceId(), "/data/datasets/" + instance.getId());
    HttpRequest request = HttpRequest.get(instanceUrl).build();
    return HttpRequests.execute(request);
  }

  private ObjectResponse<DatasetMeta> getInstanceObject(String instanceName) throws IOException {
    HttpRequest request = HttpRequest.get(getUrl("/data/datasets/" + instanceName)).build();
    HttpResponse response = HttpRequests.execute(request);
    return ObjectResponse.fromJsonBody(response, DatasetMeta.class);
  }

  private ObjectResponse<Map<String, String>> getInstanceProperties(String instanceName) throws IOException {
    HttpRequest request = HttpRequest.get(getUrl("/data/datasets/" + instanceName + "/properties")).build();
    HttpResponse response = HttpRequests.execute(request);
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }.getType());
  }

  private HttpResponse deleteInstance(String instanceName) throws IOException {
    return deleteInstance(Id.DatasetInstance.from(Id.Namespace.DEFAULT, instanceName));
  }

  private HttpResponse deleteInstance(Id.DatasetInstance instance) throws IOException {
    HttpRequest request = HttpRequest.delete(getUrl(instance.getNamespaceId(),
                                                    "/data/datasets/" + instance.getId())).build();
    return HttpRequests.execute(request);
  }

  private void deleteInstances() throws IOException {
    // NOTE: intentionally, there's no endpoint to delete all datasets instances currently to prevent bad things
    //       happen by accident
    for (DatasetSpecificationSummary spec : getInstances().getResponseObject()) {
      deleteInstance(spec.getName());
    }
  }

  private static DatasetSpecification createType2Spec(String instanceName, String typeName, String description,
                                                      DatasetProperties properties) {
    return new TestModule2().createDefinition(typeName).configure(instanceName, properties)
      .setOriginalProperties(properties).setDescription(description);
  }

  private DatasetSpecificationSummary spec2Summary(DatasetSpecification spec) {
    return new DatasetSpecificationSummary(spec.getName(), spec.getType(), spec.getDescription(),
                                           spec.getOriginalProperties());
  }

  private DatasetSpecificationSummary getSummaryForInstance(String instanceName,
                                                            List<DatasetSpecificationSummary> summaries) {
    for (DatasetSpecificationSummary summary : summaries) {
      if (instanceName.equals(summary.getName())) {
        return summary;
      }
    }
    return null;
  }

  @Test
  public void testErrorResponses() throws Exception {
    try {
      // we need at least one valid type to test
      deployModule("default-table", InMemoryTableModule.class);

      validateGet(getUrl("/data/datasets"), 200);
      validateGet(getUrl("nosuchnamespace", "/data/datasets"), 404);
      validateGet(getUrl("inval+d", "/data/datasets"), 400);
      validatePut(getUrl("/data/datasets"), "", 405);
      validatePost(getUrl("/data/datasets"), "", 405);

      validateGet(getUrl("/data/datasets/nusuch"), 404);
      validateGet(getUrl("/data/datasets/nusüch"), 400);
      validateGet(getUrl("nosuchnamespace", "/data/datasets/nusuch"), 404);
      validateGet(getUrl("nosuchnamespace", "/data/datasets/nusüch"), 400, 404);
      validateGet(getUrl("inval+d", "/data/datasets/nusuch"), 400);
      validatePost(getUrl("/data/datasets/nusuch"), "", 405);
      validatePost(getUrl("/data/datasets/nusüch"), "", 405);
      validatePost(getUrl("nosuchnamespace", "/data/datasets/nusuch"), "", 405);
      validatePost(getUrl("inval+d", "/data/datasets/nusuch"), "", 405);
      validateDelete(getUrl("/data/datasets/nusuch"), 404);
      validateDelete(getUrl("/data/datasets/nusüch"), 400);
      validateDelete(getUrl("nosuchnamespace", "/data/datasets/nusuch"), 404);
      validateDelete(getUrl("inval+d", "/data/datasets/nusuch"), 400);
      validatePut(getUrl("/data/datasets/xü"), "{'typeName':'table'}", 400);
      validatePut(getUrl("/data/datasets/x"), "{'typeName':'tablü'}", 400);
      validatePut(getUrl("/data/datasets/x"), "{'type':'table'}", 400);
      validatePut(getUrl("/data/datasets/x"), "{'type':'tab", 400);
      validatePut(getUrl("/data/datasets/x"), "", 400);
      validatePut(getUrl("nusuchns", "/data/datasets/xü"), "{'typeName':'table'}", 400, 404);
      validatePut(getUrl("nusuchns", "/data/datasets/x"), "{'typeName':'tablü'}", 400, 404);
      validatePut(getUrl("nusuchns", "/data/datasets/x"), "{'type':'table'}", 400, 404);
      validatePut(getUrl("nusuchns", "/data/datasets/x"), "{'type':'tab", 400, 404);
      validatePut(getUrl("nusuchns", "/data/datasets/x"), "", 400, 404);
      validatePut(getUrl("inval+d", "/data/datasets/xü"), "{'typeName':'table'}", 400);
      validatePut(getUrl("inval+d", "/data/datasets/x"), "{'typeName':'tablü'}", 400);
      validatePut(getUrl("inval+d", "/data/datasets/x"), "{'type':'table'}", 400);
      validatePut(getUrl("inval+d", "/data/datasets/x"), "{'type':'tab", 400);
      validatePut(getUrl("inval+d", "/data/datasets/x"), "", 400);

      // create one dataset to test PUT .../properties on it
      validatePut(getUrl("/data/datasets/x"), "{'typeName':'table'}", 200);

      // TODO (CDAP-4420): commented because netty-http throws "response already sent" for these and request times out
      // TODO: debug this in netty-http (this is not an issue with DatasetInstanceHandler, it never gets called)
      //validateGet(getUrl("/data/datasets/y/properties"), 405);
      //validatePost(getUrl("/data/datasets/x/properties"), "", 405);
      //validateDelete(getUrl("/data/datasets/x/properties"), 405);

      validatePut(getUrl("/data/datasets/y/properties"), "{'x':'y'}", 404);
      validatePut(getUrl("/data/datasets/y/properties"), "{'x':'}", 400, 404);
      validatePut(getUrl("/data/datasets/ü/properties"), "{'x':'y'}", 400);
      validatePut(getUrl("/data/datasets/ü/properties"), "{'x':'y", 400);
      validatePut(getUrl("nosuchns", "/data/datasets/x/properties"), "{}", 404);
      validatePut(getUrl("nosuchns", "/data/datasets/x/properties"), "{", 400, 404);
      validatePut(getUrl("inval+dns", "/data/datasets/x/properties"), "{'x':'y'}", 400);

      validateDelete(getUrl("/data/datasets"), 200);
    } finally {
      // cleanup
      deleteInstance("x");
      Assert.assertEquals(HttpStatus.SC_OK, deleteModules().getResponseCode());
    }
  }

  private void validateGet(URL url, Integer ... expected) throws IOException {
    HttpRequest request = HttpRequest.get(url).build();
    assertStatus(HttpRequests.execute(request).getResponseCode(), url, expected);
  }

  private void validateDelete(URL url, Integer ... expected) throws IOException {
    HttpRequest request = HttpRequest.delete(url).build();
    assertStatus(HttpRequests.execute(request).getResponseCode(), url, expected);
  }

  private void validatePut(URL url, String body, Integer ... expected) throws IOException {
    HttpRequest request = HttpRequest.put(url).withBody(body).build();
    assertStatus(HttpRequests.execute(request).getResponseCode(), url, expected);
  }

  private void validatePost(URL url, String body, Integer ... expected) throws IOException {
    HttpRequest request = HttpRequest.post(url).withBody(body).build();
    assertStatus(HttpRequests.execute(request).getResponseCode(), url, expected);
  }

  private void assertStatus(int status, URL url, Integer ... expected) {
    Assert.assertTrue(String.format("Expected %s for %s but got %d", Arrays.toString(expected), url, status),
                      Arrays.asList(expected).contains(status));
  }

}
