/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset.service;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.table.Get;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.DatasetInstanceConfiguration;
import io.cdap.cdap.proto.DatasetMeta;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.DatasetSpecificationSummary;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.http.HttpStatus;
import org.apache.tephra.DefaultTransactionExecutor;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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
  public void testBasics() throws Exception {
    // nothing has been created, modules and types list is empty
    List<DatasetSpecificationSummary> instances = getInstances().getResponseObject();

    // nothing in the beginning
    Assert.assertEquals(0, instances.size());

    try {
      // create dataset instance with type that is not known to the system should fail
      DatasetProperties props = DatasetProperties.builder().add("prop1", "val1").build();
      Assert.assertEquals(HttpStatus.SC_NOT_FOUND, createInstance("dataset1", "unknownType", props).getResponseCode());

      // create dataset instance
      String description = "test instance description";
      HttpResponse response = createInstance("dataset1", "datasetType2", description, props);
      Assert.assertEquals(HttpStatus.SC_OK, response.getResponseCode());

      // Verify that dataset instance can be retrieved using specified properties
      Map<String, String> properties = new HashMap<>(props.getProperties());
      instances = getInstancesWithProperties(NamespaceId.DEFAULT.getNamespace(), properties).getResponseObject();
      Assert.assertEquals(1, instances.size());
      properties.put("some_prop_not_associated_with_dataset", "somevalue");
      instances = getInstancesWithProperties(NamespaceId.DEFAULT.getNamespace(), properties).getResponseObject();
      Assert.assertEquals(0, instances.size());

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
                                    Collections.emptyList(), ImmutableList.of("module2"));
      DatasetTypeHandlerTest.verify(modules.get(1), "module2", TestModule2.class, ImmutableList.of("datasetType2"),
                                    ImmutableList.of("module1"), Collections.emptyList());

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
    } finally {
      deleteInstance("dataset1");
      deleteInstance("nullPropertiesTable");
      deleteInstance("localDSInstance");
    }
  }

  @Test
  public void testOwner() throws Exception {
    // should not be able to create a dataset with invalid kerberos principal format
    HttpResponse response = createInstance(NamespaceId.DEFAULT.dataset("ownedDataset"), "datasetType1", null,
                                           DatasetProperties.EMPTY, "alice/bob/somehost.net@somekdc.net");
    Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getResponseCode());

    // should be able to create a dataset with valid kerberos principal format
    String alicePrincipal = "alice/somehost.net@somekdc.net";
    response = createInstance(NamespaceId.DEFAULT.dataset("ownedDataset"), "datasetType1", null,
                              DatasetProperties.EMPTY,
                              alicePrincipal);
    Assert.assertEquals(HttpStatus.SC_OK, response.getResponseCode());

    // owner information should have stored
    Assert.assertEquals(alicePrincipal, ownerAdmin.getOwnerPrincipal(NamespaceId.DEFAULT.dataset("ownedDataset")));

    // should be able to retrieve owner information back
    DatasetMeta meta = getInstanceObject("ownedDataset").getResponseObject();
    Assert.assertEquals(alicePrincipal, meta.getOwnerPrincipal());


    // deleting the dataset should delete the owner information
    response = deleteInstance(NamespaceId.DEFAULT.dataset("ownedDataset"));
    Assert.assertEquals(HttpStatus.SC_OK, response.getResponseCode());
    Assert.assertNull(ownerAdmin.getOwner(NamespaceId.DEFAULT.dataset("ownedDataset")));
  }

  @Test
  public void testInvalidProperties() throws Exception {
    // creating a partitionedFileSet without the appropriate properties (partitioning) should return a 400
    HttpResponse response = createInstance("dataset1", "partitionedFileSet", DatasetProperties.EMPTY);
    Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getResponseCode());
    // ensure that we get the error we expect
    Assert.assertEquals("Properties do not contain partitioning", response.getResponseBodyAsString());
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
    }
  }

  @Test
  public void testCreateDelete() throws Exception {
    try {
      // cannot create instance with same name again
      Assert.assertEquals(HttpStatus.SC_OK,
                          createInstance("myTable1", "table", DatasetProperties.EMPTY).getResponseCode());
      Assert.assertEquals(HttpStatus.SC_OK,
                          createInstance("myTable2", "table", DatasetProperties.EMPTY).getResponseCode());
      Assert.assertEquals(2, getInstances().getResponseObject().size());

      // we want to verify that data is also gone, so we write smth to tables first
      final Table table1 = dsFramework.getDataset(NamespaceId.DEFAULT.dataset("myTable1"),
                                                  DatasetDefinition.NO_ARGUMENTS, null);
      final Table table2 = dsFramework.getDataset(NamespaceId.DEFAULT.dataset("myTable2"),
                                                  DatasetDefinition.NO_ARGUMENTS, null);
      Assert.assertNotNull(table1);
      Assert.assertNotNull(table2);
      TransactionExecutor txExecutor =
        new DefaultTransactionExecutor(new InMemoryTxSystemClient(txManager),
                                       ImmutableList.of((TransactionAware) table1, (TransactionAware) table2));

      txExecutor.execute(() -> {
        table1.put(new Put("key1", "col1", "val1"));
        table2.put(new Put("key2", "col2", "val2"));
      });

      // verify that we can read the data
      txExecutor.execute(() -> {
        Assert.assertEquals("val1", table1.get(new Get("key1", "col1")).getString("col1"));
        Assert.assertEquals("val2", table2.get(new Get("key2", "col2")).getString("col2"));
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
      txExecutor.execute(() -> {
        Assert.assertTrue(table1.get(new Get("key1", "col1")).isEmpty());
        Assert.assertEquals("val2", table2.get(new Get("key2", "col2")).getString("col2"));
        // writing smth to table1 for subsequent test
        table1.put(new Put("key3", "col3", "val3"));
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
      txExecutor.execute(() -> {
        Assert.assertTrue(table1.get(new Get("key3", "col3")).isEmpty());
        Assert.assertTrue(table2.get(new Get("key2", "col2")).isEmpty());
      });
    } finally {
      // cleanup
      deleteInstances();
    }
  }

  /**
   * Tests the operations on existing datasets whose type is defined by a dataset module that is not
   * available in the classpath.
   */
  @Test
  public void testCustomTypeDataset() throws Exception {
    Location moduleJar = createModuleJar("customModule");
    DatasetModuleMeta customModule =
      new DatasetModuleMeta("customModule", "io.cdap.cdap.test.NotInClasspathModule", moduleJar.toURI(),
                            ImmutableList.of("customType"), Collections.emptyList());
    DatasetProperties props = DatasetProperties.builder().add("prop1", "val1").build();
    DatasetSpecification customSpec = DatasetSpecification.builder("customDataset", "customType")
      .properties(props.getProperties()).build().setOriginalProperties(props);
    try {
      writeModule(NamespaceId.DEFAULT, customModule);
      writeInstance(NamespaceId.DEFAULT, customSpec);

      // the dataset is listed, and its meta and properties can be retrieved
      List<DatasetSpecificationSummary> instances = getInstances().getResponseObject();
      Assert.assertEquals(1, instances.size());
      Assert.assertEquals(spec2Summary(customSpec), instances.get(0));
      DatasetMeta datasetInfo = getInstanceObject("customDataset").getResponseObject();
      Assert.assertEquals(customSpec, datasetInfo.getSpec());
      Assert.assertEquals("customType", datasetInfo.getType().getName());
      Assert.assertEquals(props.getProperties(), getInstanceProperties("customDataset").getResponseObject());

      // the dataset cannot be updated, and no admin operation can be executed on it
      assertCustomTypeNotSupported(updateInstance("customDataset", DatasetProperties.EMPTY), "customType");
      assertCustomTypeNotSupported(executeAdmin("customDataset", "exists"), "customType");
      assertCustomTypeNotSupported(executeAdmin("customDataset", "truncate"), "customType");
      assertCustomTypeNotSupported(executeAdmin("customDataset", "upgrade"), "customType");
      Assert.assertEquals(customSpec, getInstanceObject("customDataset").getResponseObject().getSpec());

      // the dataset cannot be instantiated
      try {
        dsFramework.getDataset(NamespaceId.DEFAULT.dataset("customDataset"), DatasetDefinition.NO_ARGUMENTS, null);
        Assert.fail("Should not be able to instantiate a dataset of a custom type");
      } catch (DatasetManagementException e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("custom dataset types are not supported"));
      }
      try {
        dsFramework.getAdmin(NamespaceId.DEFAULT.dataset("customDataset"), null);
        Assert.fail("Should not be able to instantiate the admin of a dataset of a custom type");
      } catch (DatasetManagementException e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("custom dataset types are not supported"));
      }

      // cannot create another dataset of that type
      assertCustomTypeNotSupported(createInstance("customDataset2", "customType", props), "customType");
      Assert.assertEquals(1, getInstances().getResponseObject().size());

      // the dataset can be deleted, after that the module can be deleted
      Assert.assertEquals(HttpStatus.SC_CONFLICT, deleteModule("customModule").getResponseCode());
      Assert.assertEquals(HttpStatus.SC_OK, deleteInstance("customDataset").getResponseCode());
      Assert.assertEquals(HttpStatus.SC_NOT_FOUND, getInstance("customDataset").getResponseCode());
      Assert.assertEquals(0, getInstances().getResponseObject().size());
      Assert.assertEquals(HttpStatus.SC_OK, deleteModule("customModule").getResponseCode());
      Assert.assertFalse(moduleJar.exists());
    } finally {
      deleteInstance("customDataset");
      deleteModule("customModule");
    }
  }

  /**
   * Tests that a dataset type of a namespace takes precedence over a system type of the same name for the
   * existing datasets of that namespace.
   */
  @Test
  public void testCustomTypeWithSystemTypeName() throws Exception {
    Location moduleJar = createModuleJar("sameNameModule");
    DatasetModuleMeta sameNameModule =
      new DatasetModuleMeta("sameNameModule", TestModule1.class.getName(), moduleJar.toURI(),
                            ImmutableList.of("datasetType1"), Collections.emptyList());
    try {
      // create a dataset of the system type
      Assert.assertEquals(HttpStatus.SC_OK, createInstance("dataset1", "datasetType1").getResponseCode());
      Assert.assertEquals(HttpStatus.SC_OK,
                          updateInstance("dataset1", DatasetProperties.EMPTY).getResponseCode());

      // after a module with a type of the same name exists in the namespace, the dataset can only be deleted
      writeModule(NamespaceId.DEFAULT, sameNameModule);
      Assert.assertEquals(HttpStatus.SC_OK, getInstance("dataset1").getResponseCode());
      assertCustomTypeNotSupported(updateInstance("dataset1", DatasetProperties.EMPTY), "datasetType1");
      assertCustomTypeNotSupported(executeAdmin("dataset1", "truncate"), "datasetType1");
      assertCustomTypeNotSupported(createInstance("dataset2", "datasetType1"), "datasetType1");
      Assert.assertEquals(HttpStatus.SC_OK, deleteInstance("dataset1").getResponseCode());
      Assert.assertEquals(HttpStatus.SC_OK, deleteModule("sameNameModule").getResponseCode());

      // the system type can be used again
      Assert.assertEquals(HttpStatus.SC_OK, createInstance("dataset1", "datasetType1").getResponseCode());
    } finally {
      deleteInstance("dataset1");
      deleteModule("sameNameModule");
    }
  }

  /**
   * Tests that an existing dataset can be deleted if its type does not exist.
   */
  @Test
  public void testDeleteDatasetOfMissingType() throws Exception {
    writeInstance(NamespaceId.DEFAULT, DatasetSpecification.builder("orphanDataset", "missingType").build());
    try {
      Assert.assertEquals(1, getInstances().getResponseObject().size());
      Assert.assertEquals(HttpStatus.SC_NOT_FOUND, getInstance("orphanDataset").getResponseCode());
      Assert.assertEquals(HttpStatus.SC_OK, deleteInstance("orphanDataset").getResponseCode());
      Assert.assertEquals(0, getInstances().getResponseObject().size());
    } finally {
      deleteInstance("orphanDataset");
    }
  }

  private void assertCustomTypeNotSupported(HttpResponse response, String typeName) {
    Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getResponseCode());
    Assert.assertEquals(
      String.format("Dataset type '%s' is a custom type; custom dataset types are not supported", typeName),
      response.getResponseBodyAsString());
  }

  private HttpResponse executeAdmin(String instanceName, String method) throws IOException {
    HttpRequest request = HttpRequest.post(
      getUrl("/data/datasets/" + instanceName + "/admin/" + method)).build();
    return HttpRequests.execute(request, REQUEST_CONFIG);
  }

  @Test
  public void testNotFound() throws IOException {
    NamespaceId nonExistent = new NamespaceId("nonexistent");
    DatasetId datasetInstance = nonExistent.dataset("ds");

    HttpResponse response = makeInstancesRequest(nonExistent.getNamespace());
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
    response = getInstances(NamespaceId.SYSTEM.getNamespace());
    Assert.assertEquals(200, response.getResponseCode());
  }

  private HttpResponse createInstance(String instanceName, String typeName,
                                      DatasetProperties props) throws IOException {
    return createInstance(NamespaceId.DEFAULT.dataset(instanceName), typeName, props);
  }

  private HttpResponse createInstance(String instanceName, String typeName, String description,
                                      DatasetProperties props) throws IOException {
    return createInstance(NamespaceId.DEFAULT.dataset(instanceName), typeName, description, props, null);
  }

  private HttpResponse createInstance(String instanceName, String typeName) throws IOException {
    return createInstance(NamespaceId.DEFAULT.dataset(instanceName), typeName, null);
  }

  private HttpResponse createInstance(DatasetId instance, String typeName,
                                      @Nullable DatasetProperties props) throws IOException {
    return createInstance(instance, typeName, null, props, null);
  }

  private HttpResponse createInstance(DatasetId instance, String typeName, @Nullable String description,
                                      @Nullable DatasetProperties props,
                                      @Nullable String ownerPrincipal) throws IOException {
    DatasetInstanceConfiguration creationProperties;
    if (props != null) {
      creationProperties = new DatasetInstanceConfiguration(typeName, props.getProperties(), description,
                                                            ownerPrincipal);
    } else {
      creationProperties = new DatasetInstanceConfiguration(typeName, null, description, ownerPrincipal);
    }
    HttpRequest request = HttpRequest.put(getUrl(instance.getNamespace(), "/data/datasets/" + instance.getEntityName()))
      .withBody(GSON.toJson(creationProperties)).build();
    return HttpRequests.execute(request, REQUEST_CONFIG);
  }

  private HttpResponse updateInstance(String instanceName, DatasetProperties props) throws IOException {
    return updateInstance(NamespaceId.DEFAULT.dataset(instanceName), props);
  }

  private HttpResponse updateInstance(DatasetId instance, DatasetProperties props) throws IOException {
    HttpRequest request = HttpRequest.put(getUrl(instance.getNamespace(),
                                                 "/data/datasets/" + instance.getEntityName() + "/properties"))
      .withBody(GSON.toJson(props.getProperties())).build();
    return HttpRequests.execute(request, REQUEST_CONFIG);
  }

  private ObjectResponse<List<DatasetSpecificationSummary>> getInstances() throws IOException {
    return getInstances(NamespaceId.DEFAULT.getEntityName());
  }

  private ObjectResponse<List<DatasetSpecificationSummary>> getInstances(String namespace) throws IOException {
    return ObjectResponse.fromJsonBody(makeInstancesRequest(namespace),
                                       new TypeToken<List<DatasetSpecificationSummary>>() { }.getType());
  }

  private HttpResponse makeInstancesRequest(String namespace) throws IOException {
    HttpRequest request = HttpRequest.get(getUrl(namespace, "/data/datasets")).build();
    return HttpRequests.execute(request, REQUEST_CONFIG);
  }

  private ObjectResponse<List<DatasetSpecificationSummary>> getInstancesWithProperties(String namespace,
                                                                                       Map<String, String> properties)
    throws IOException {
    HttpRequest request = HttpRequest.post(getUrl(namespace, "/data/datasets"))
      .withBody(GSON.toJson(properties)).build();
    return ObjectResponse.fromJsonBody(HttpRequests.execute(request, REQUEST_CONFIG),
                                       new TypeToken<List<DatasetSpecificationSummary>>() { }.getType());
  }

  private HttpResponse getInstance(String instanceName) throws IOException {
    return getInstance(NamespaceId.DEFAULT.dataset(instanceName));
  }

  private HttpResponse getInstance(DatasetId instance) throws IOException {
    URL instanceUrl = getUrl(instance.getNamespace(), "/data/datasets/" + instance.getEntityName());
    HttpRequest request = HttpRequest.get(instanceUrl).build();
    return HttpRequests.execute(request, REQUEST_CONFIG);
  }

  private ObjectResponse<DatasetMeta> getInstanceObject(String instanceName) throws IOException {
    HttpRequest request = HttpRequest.get(getUrl("/data/datasets/" + instanceName)).build();
    HttpResponse response = HttpRequests.execute(request, REQUEST_CONFIG);
    return ObjectResponse.fromJsonBody(response, DatasetMeta.class);
  }

  private ObjectResponse<Map<String, String>> getInstanceProperties(String instanceName) throws IOException {
    HttpRequest request = HttpRequest.get(getUrl("/data/datasets/" + instanceName + "/properties")).build();
    HttpResponse response = HttpRequests.execute(request, REQUEST_CONFIG);
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }.getType());
  }

  private HttpResponse deleteInstance(String instanceName) throws IOException {
    return deleteInstance(NamespaceId.DEFAULT.dataset(instanceName));
  }

  private HttpResponse deleteInstance(DatasetId instance) throws IOException {
    HttpRequest request = HttpRequest.delete(getUrl(instance.getNamespace(),
                                                    "/data/datasets/" + instance.getEntityName())).build();
    return HttpRequests.execute(request, REQUEST_CONFIG);
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
      validateGet(getUrl("/data/datasets"), 200);
      validateGet(getUrl("nosuchnamespace", "/data/datasets"), 404);
      validateGet(getUrl("inval+d", "/data/datasets"), 400);
      validatePost(getUrl("/data/datasets"), "", 200);
      validatePut(getUrl("/data/datasets"), "", 405);

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
    }
  }

  private void validateGet(URL url, Integer ... expected) throws IOException {
    HttpRequest request = HttpRequest.get(url).build();
    assertStatus(HttpRequests.execute(request, REQUEST_CONFIG).getResponseCode(), url, expected);
  }

  private void validateDelete(URL url, Integer ... expected) throws IOException {
    HttpRequest request = HttpRequest.delete(url).build();
    assertStatus(HttpRequests.execute(request, REQUEST_CONFIG).getResponseCode(), url, expected);
  }

  private void validatePut(URL url, String body, Integer ... expected) throws IOException {
    HttpRequest request = HttpRequest.put(url).withBody(body).build();
    assertStatus(HttpRequests.execute(request, REQUEST_CONFIG).getResponseCode(), url, expected);
  }

  private void validatePost(URL url, String body, Integer ... expected) throws IOException {
    HttpRequest request = HttpRequest.post(url).withBody(body).build();
    assertStatus(HttpRequests.execute(request, REQUEST_CONFIG).getResponseCode(), url, expected);
  }

  private void assertStatus(int status, URL url, Integer ... expected) {
    Assert.assertTrue(String.format("Expected %s for %s but got %d", Arrays.toString(expected), url, status),
                      Arrays.asList(expected).contains(status));
  }

}
