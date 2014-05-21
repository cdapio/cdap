package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.data2.datafabric.dataset.type.DatasetModuleMeta;
import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.internal.data.dataset.Dataset;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Unit-test for {@link com.continuuity.data2.datafabric.dataset.service.DatasetInstanceHandler}
 */
public class DatasetInstanceHandlerTest extends DatasetManagerServiceTestBase {

  @Test
  public void testBasics() throws Exception {

    // nothing has been created, modules and types list is empty
    List<DatasetInstanceSpec> instances = getInstances().value;

    // nothing in the beginning
    Assert.assertEquals(0, instances.size());

    // create dataset instance with type that is not yet known to the system should fail
    DatasetInstanceProperties props = new DatasetInstanceProperties.Builder().property("prop1", "val1").build();
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, createInstance("dataset1", "datasetType2", props));

    // deploy modules
    deployModule("module1", TestModule1.class);
    deployModule("module2", TestModule2.class);

    // create dataset instance
    Assert.assertEquals(HttpStatus.SC_OK, createInstance("dataset1", "datasetType2", props));

    // verify instance was created
    instances = getInstances().value;
    Assert.assertEquals(1, instances.size());
    // verifying spec is same as expected
    DatasetInstanceSpec dataset1Spec = createSpec("dataset1", "datasetType2", props);
    Assert.assertEquals(dataset1Spec, instances.get(0));

    // verify created instance info can be retrieved
    DatasetInstanceMeta datasetInfo = getInstance("dataset1").value;
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
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, getInstance("non-existing-dataset").status);

    // cannot create instance with same name again
    Assert.assertEquals(HttpStatus.SC_CONFLICT, createInstance("dataset1", "datasetType2", props));
    Assert.assertEquals(1, getInstances().value.size());

    // cannot delete non-existing dataset instance
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, deleteInstance("non-existing-dataset"));
    Assert.assertEquals(1, getInstances().value.size());

    // delete dataset instance
    Assert.assertEquals(HttpStatus.SC_OK, deleteInstance("dataset1"));
    Assert.assertEquals(0, getInstances().value.size());

    // delete dataset modules
    Assert.assertEquals(HttpStatus.SC_OK, deleteModule("module2"));
    Assert.assertEquals(HttpStatus.SC_OK, deleteModule("module1"));
  }

  private int createInstance(String instanceName, String typeName, DatasetInstanceProperties props) throws IOException {
    HttpPost post = new HttpPost(getUrl("/datasets/instances/" + instanceName));
    post.addHeader("type-name", typeName);
    post.setEntity(new StringEntity(new Gson().toJson(props)));

    DefaultHttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(post);

    return response.getStatusLine().getStatusCode();
  }

  private Response<List<DatasetInstanceSpec>> getInstances() throws IOException {
    HttpGet get = new HttpGet(getUrl("/datasets/instances"));
    DefaultHttpClient client = new DefaultHttpClient();
    return parseResponse(client.execute(get), new TypeToken<List<DatasetInstanceSpec>>() { }.getType());
  }

  private Response<DatasetInstanceMeta> getInstance(String instanceName) throws IOException {
    HttpGet get = new HttpGet(getUrl("/datasets/instances/" + instanceName));
    DefaultHttpClient client = new DefaultHttpClient();
    return parseResponse(client.execute(get), DatasetInstanceMeta.class);
  }

  private int deleteInstance(String instanceName) throws IOException {
    HttpDelete delete = new HttpDelete(getUrl("/datasets/instances/" + instanceName));
    HttpResponse response = new DefaultHttpClient().execute(delete);
    return response.getStatusLine().getStatusCode();
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
      public DatasetInstanceSpec configure(String instanceName, DatasetInstanceProperties properties) {
        return createSpec(instanceName, getName(), properties);
      }

      @Override
      public DatasetAdmin getAdmin(DatasetInstanceSpec spec) {
        return null;
      }

      @Override
      public Dataset getDataset(DatasetInstanceSpec spec) {
        return null;
      }
    };
  }

  private static DatasetInstanceSpec createSpec(String instanceName, String typeName,
                                                DatasetInstanceProperties properties) {
    return new DatasetInstanceSpec.Builder(instanceName, typeName).properties(properties.getProperties()).build();
  }
}
