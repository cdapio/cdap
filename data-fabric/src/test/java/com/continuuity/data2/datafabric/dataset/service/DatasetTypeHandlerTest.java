package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDatasetDefinition;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.common.http.HttpRequests;
import com.continuuity.common.http.ObjectResponse;
import com.continuuity.data2.datafabric.dataset.type.DatasetModuleMeta;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Unit-test for {@link DatasetTypeHandler}
 */
public class DatasetTypeHandlerTest extends DatasetServiceTestBase {

  @Test
  public void testBasics() throws Exception {
    // nothing has been deployed, modules and types list is empty
    List<DatasetModuleMeta> modules = getModules().getResponseObject();
    Assert.assertEquals(0, modules.size());
    List<DatasetTypeMeta> types = getTypes().getResponseObject();
    Assert.assertEquals(0, types.size());

    // deploy module
    Assert.assertEquals(HttpStatus.SC_OK, deployModule("module1", TestModule1.class));

    // verify deployed module present in a list
    modules = getModules().getResponseObject();
    Assert.assertEquals(1, modules.size());
    verify(modules.get(0),
           "module1", TestModule1.class, ImmutableList.of("datasetType1"),
           Collections.<String>emptyList(), Collections.<String>emptyList());

    // verify deployed module info can be retrieved
    verify(getModule("module1").getResponseObject(), "module1", TestModule1.class, ImmutableList.of("datasetType1"),
           Collections.<String>emptyList(), Collections.<String>emptyList());
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, getType("datasetType2").getResponseCode());

    // verify type information can be retrieved
    verify(getType("datasetType1").getResponseObject(), "datasetType1", ImmutableList.of("module1"));
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, getType("datasetType2").getResponseCode());

    types = getTypes().getResponseObject();
    Assert.assertEquals(1, types.size());
    verify(types.get(0), "datasetType1", ImmutableList.of("module1"));


    // cannot deploy same module again
    Assert.assertEquals(HttpStatus.SC_CONFLICT, deployModule("module1", TestModule1.class));
    // cannot deploy module with same types
    Assert.assertEquals(HttpStatus.SC_CONFLICT, deployModule("not-module1", TestModule1.class));

    // deploy another module which depends on the first one
    Assert.assertEquals(HttpStatus.SC_OK, deployModule("module2", TestModule2.class));

    // verify deployed module present in a list
    modules = getModules().getResponseObject();
    Assert.assertEquals(2, modules.size());
    for (DatasetModuleMeta module : modules) {
      if ("module1".equals(module.getName())) {
        verify(module, "module1", TestModule1.class, ImmutableList.of("datasetType1"),
               Collections.<String>emptyList(), ImmutableList.of("module2"));
      } else if ("module2".equals(module.getName())) {
        verify(module, "module2", TestModule2.class, ImmutableList.of("datasetType2"),
               ImmutableList.of("module1"), Collections.<String>emptyList());
      } else {
        Assert.fail("unexpected module: " + module);
      }
    }

    // verify deployed module info can be retrieved
    verify(getModule("module2").getResponseObject(), "module2", TestModule2.class, ImmutableList.of("datasetType2"),
           ImmutableList.of("module1"), Collections.<String>emptyList());

    // verify type information can be retrieved
    verify(getType("datasetType1").getResponseObject(), "datasetType1", ImmutableList.of("module1"));
    verify(getType("datasetType2").getResponseObject(), "datasetType2", ImmutableList.of("module1", "module2"));

    types = getTypes().getResponseObject();
    Assert.assertEquals(2, types.size());
    for (DatasetTypeMeta type : types) {
      if ("datasetType1".equals(type.getName())) {
        verify(type, "datasetType1", ImmutableList.of("module1"));
      } else if ("datasetType2".equals(type.getName())) {
        verify(type, "datasetType2", ImmutableList.of("module1", "module2"));
      } else {
        Assert.fail("unexpected type: " + type);
      }
    }

    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, deleteModule("non-existing-module"));
    // cannot delete module1 since module2 depends on it, verify that nothing has been deleted
    Assert.assertEquals(HttpStatus.SC_CONFLICT, deleteModule("module1"));
    verify(getModule("module1").getResponseObject(), "module1", TestModule1.class, ImmutableList.of("datasetType1"),
           Collections.<String>emptyList(), ImmutableList.of("module2"));
    verify(getType("datasetType1").getResponseObject(), "datasetType1", ImmutableList.of("module1"));
    Assert.assertEquals(2, getTypes().getResponseObject().size());

    // delete module2, should be removed from usedBy list everywhere and all its types should no longer be available
    Assert.assertEquals(HttpStatus.SC_OK, deleteModule("module2"));
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, getType("datasetType2").getResponseCode());
    verify(getModule("module1").getResponseObject(), "module1", TestModule1.class, ImmutableList.of("datasetType1"),
           Collections.<String>emptyList(), Collections.<String>emptyList());

    Assert.assertEquals(1, getModules().getResponseObject().size());
    Assert.assertEquals(1, getTypes().getResponseObject().size());

    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, deleteModule("module2"));
    Assert.assertEquals(HttpStatus.SC_OK, deleteModules());
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, getType("datasetType1").getResponseCode());

    Assert.assertEquals(0, getModules().getResponseObject().size());
    Assert.assertEquals(0, getTypes().getResponseObject().size());
  }

  private void verify(DatasetTypeMeta typeMeta, String typeName, List<String> modules) {
    Assert.assertEquals(typeName, typeMeta.getName());
    Assert.assertArrayEquals(modules.toArray(),
                             Lists.transform(typeMeta.getModules(),
                                             new Function<DatasetModuleMeta, String>() {
                                               @Nullable
                                               @Override
                                               public String apply(@Nullable DatasetModuleMeta input) {
                                                 return input == null ? null : input.getName();
                                               }
                                             }).toArray());
  }

  static void verify(DatasetModuleMeta moduleMeta, String moduleName, Class moduleClass,
                     List<String> types, List<String> usesModules, Collection<String> usedByModules) {
    Assert.assertEquals(moduleName, moduleMeta.getName());
    Assert.assertEquals(moduleClass.getName(), moduleMeta.getClassName());
    Assert.assertArrayEquals(types.toArray(), moduleMeta.getTypes().toArray());
    Assert.assertArrayEquals(usesModules.toArray(), moduleMeta.getUsesModules().toArray());
    // using treeset just for sorting purposes
    Assert.assertArrayEquals(Sets.newTreeSet(usedByModules).toArray(),
                             Sets.newTreeSet(moduleMeta.getUsedByModules()).toArray());
    // note: we know it is local ;)
    Assert.assertTrue(new File(moduleMeta.getJarLocation()).exists());
  }

  private ObjectResponse<List<DatasetTypeMeta>> getTypes() throws IOException {
    return ObjectResponse.fromJsonBody(HttpRequests.get(getUrl("/data/types")),
                                       new TypeToken<List<DatasetTypeMeta>>() {
                                       }.getType());
  }

  private ObjectResponse<DatasetModuleMeta> getModule(String moduleName) throws IOException {
    return ObjectResponse.fromJsonBody(HttpRequests.get(getUrl("/data/modules/" + moduleName)),
                                       DatasetModuleMeta.class);
  }

  private ObjectResponse<DatasetTypeMeta> getType(String typeName) throws IOException {
    return ObjectResponse.fromJsonBody(HttpRequests.get(getUrl("/data/types/" + typeName)),
                                       DatasetTypeMeta.class);
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
      Assert.assertNotNull(registry.get("datasetType1"));
      registry.add(createDefinition("datasetType2"));
    }
  }

  private static DatasetDefinition createDefinition(String name) {
    return new AbstractDatasetDefinition(name) {
      @Override
      public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
        return null;
      }

      @Override
      public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) {
        return null;
      }

      @Override
      public Dataset getDataset(DatasetSpecification spec, ClassLoader classLoader) {
        return null;
      }
    };
  }
}
