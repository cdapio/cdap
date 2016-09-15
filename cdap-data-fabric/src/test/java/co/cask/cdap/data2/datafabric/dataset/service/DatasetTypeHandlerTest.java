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

import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpStatus;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Unit-test for {@link DatasetTypeHandler}
 */
public class DatasetTypeHandlerTest extends DatasetServiceTestBase {

  private static final DatasetModuleMeta MODULE1X_UNUSED =
    new DatasetModuleMeta("module1", TestModule1x.class.getName(), null,
                          ImmutableList.of("datasetType1", "datasetType1x"),
                          Collections.<String>emptyList());

  private static final DatasetModuleMeta MODULE1X_USED =
    new DatasetModuleMeta("module1", TestModule1x.class.getName(), null,
                          ImmutableList.of("datasetType1", "datasetType1x"),
                          Collections.<String>emptyList(), ImmutableList.of("module2"));

  private static final DatasetModuleMeta MODULE1_UNUSED =
    new DatasetModuleMeta("module1", TestModule1.class.getName(), null,
                          ImmutableList.of("datasetType1"),
                          Collections.<String>emptyList());

  private static final DatasetModuleMeta MODULE1_USED =
    new DatasetModuleMeta("module1", TestModule1.class.getName(), null,
                          ImmutableList.of("datasetType1"),
                          Collections.<String>emptyList(), ImmutableList.of("module2"));

  private static final DatasetModuleMeta MODULE2 =
    new DatasetModuleMeta("module2", TestModule2.class.getName(), null,
                          ImmutableList.of("datasetType2"),
                          ImmutableList.of("module1"));



  private static final Set<DatasetModuleMeta> NO_MODULES = Collections.emptySet();
  private static final Set<DatasetModuleMeta> ONLY_MODULE1 = ImmutableSet.of(MODULE1_UNUSED);
  private static final Set<DatasetModuleMeta> ONLY_MODULE1X = ImmutableSet.of(MODULE1X_UNUSED);
  private static final Set<DatasetModuleMeta> MODULES_1_AND_2 = ImmutableSet.of(MODULE1_USED, MODULE2);
  private static final Set<DatasetModuleMeta> MODULES_1X_AND_2 = ImmutableSet.of(MODULE1X_USED, MODULE2);

  private static final Map<String, List<String>> NO_DEPENDENCIES = Collections.emptyMap();
  private static final Map<String, List<String>> ONLY_1_DEPENDENCIES =
    ImmutableMap.<String, List<String>>of("datasetType1", ImmutableList.of("module1"));
  private static final Map<String, List<String>> ONLY_1X_DEPENDENCIES =
    ImmutableMap.<String, List<String>>of("datasetType1", ImmutableList.of("module1"),
                                          "datasetType1x", ImmutableList.of("module1"));
  private static final Map<String, List<String>> BOTH_1_2_DEPENDENCIES =
    ImmutableMap.<String, List<String>>of("datasetType1", ImmutableList.of("module1"),
                                          "datasetType2", ImmutableList.of("module1", "module2"));
  private static final Map<String, List<String>> BOTH_1X_2_DEPENDENCIES =
    ImmutableMap.<String, List<String>>of("datasetType1", ImmutableList.of("module1"),
                                          "datasetType1x", ImmutableList.of("module1"),
                                          "datasetType2", ImmutableList.of("module1", "module2"));


  @BeforeClass
  public static void setup() throws Exception {
    DatasetServiceTestBase.initialize();
  }

  @Test
  public void testBasics() throws Exception {
    // nothing has been deployed, modules and types list is empty
    verifyAll(NO_MODULES, NO_DEPENDENCIES);

    // deploy module and verify that it can be retrieved in all ways
    Assert.assertEquals(HttpStatus.SC_OK, deployModule("module1", TestModule1x.class).getResponseCode());
    verifyAll(ONLY_MODULE1X, ONLY_1X_DEPENDENCIES);

    // cannot deploy other module with same types
    Assert.assertEquals(HttpStatus.SC_CONFLICT, deployModule("not-module1", TestModule1.class).getResponseCode());
    verifyAll(ONLY_MODULE1X, ONLY_1X_DEPENDENCIES);

    // can deploy same module again
    Assert.assertEquals(HttpStatus.SC_OK, deployModule("module1", TestModule1x.class).getResponseCode());
    verifyAll(ONLY_MODULE1X, ONLY_1X_DEPENDENCIES);

    // create a dataset instance, verify that we cannot redeploy the module with fewer types, even with force option
    instanceService.create(NamespaceId.DEFAULT.getNamespace(), "instance1x",
                           new DatasetInstanceConfiguration("datasetType1x", new HashMap<String, String>()));
    Assert.assertEquals(HttpStatus.SC_CONFLICT, deployModule("module1", TestModule1.class).getResponseCode());
    Assert.assertEquals(HttpStatus.SC_CONFLICT, deployModule("module1", TestModule1.class, true).getResponseCode());
    verifyAll(ONLY_MODULE1X, ONLY_1X_DEPENDENCIES);

    // drop the instance, now we should be able to redeploy, dropping type1x
    instanceService.drop(NamespaceId.DEFAULT.dataset("instance1x"));
    Assert.assertEquals(HttpStatus.SC_OK, deployModule("module1", TestModule1.class).getResponseCode());
    verifyAll(ONLY_MODULE1, ONLY_1_DEPENDENCIES);

    // redeploy module with more types and validate
    Assert.assertEquals(HttpStatus.SC_OK, deployModule("module1", TestModule1x.class).getResponseCode());
    verifyAll(ONLY_MODULE1X, ONLY_1X_DEPENDENCIES);

    // deploy another module which depends on the first one
    Assert.assertEquals(HttpStatus.SC_OK, deployModule("module2", TestModule2.class).getResponseCode());
    verifyAll(MODULES_1X_AND_2, BOTH_1X_2_DEPENDENCIES);

    // cannot delete non-existent module
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, deleteModule("non-existing-module").getResponseCode());

    // cannot delete module1 since module2 depends on it, verify that nothing has been deleted
    Assert.assertEquals(HttpStatus.SC_CONFLICT, deleteModule("module1").getResponseCode());
    verifyAll(MODULES_1X_AND_2, BOTH_1X_2_DEPENDENCIES);

    // cannot deploy same module again with fewer types (because module2 depends on it)
    Assert.assertEquals(HttpStatus.SC_CONFLICT, deployModule("module1", TestModule1.class).getResponseCode());
    verifyAll(MODULES_1X_AND_2, BOTH_1X_2_DEPENDENCIES);

    // create dataset instances, try force deploy of same module again with fewer types - should fail
    instanceService.create(NamespaceId.DEFAULT.getNamespace(), "instance1",
                           new DatasetInstanceConfiguration("datasetType1", new HashMap<String, String>()));
    instanceService.create(NamespaceId.DEFAULT.getNamespace(), "instance1x",
                           new DatasetInstanceConfiguration("datasetType1x", new HashMap<String, String>()));
    Assert.assertEquals(HttpStatus.SC_CONFLICT, deployModule("module1", TestModule1.class, true).getResponseCode());
    verifyAll(MODULES_1X_AND_2, BOTH_1X_2_DEPENDENCIES);

    // drop the instance of type1x, now forced deploy should work
    instanceService.drop(NamespaceId.DEFAULT.dataset("instance1x"));
    Assert.assertEquals(HttpStatus.SC_OK, deployModule("module1", TestModule1.class, true).getResponseCode());
    verifyAll(MODULES_1_AND_2, BOTH_1_2_DEPENDENCIES);

    // delete module2, should be removed from usedBy list everywhere and all its types should no longer be available
    Assert.assertEquals(HttpStatus.SC_OK, deleteModule("module2").getResponseCode());
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, getMissingType("datasetType2").getResponseCode());
    verifyAll(ONLY_MODULE1, ONLY_1_DEPENDENCIES);

    // cannot delete module2 again
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, deleteModule("module2").getResponseCode());

    // cannot delete module 1 becuse there is an instance
    Assert.assertEquals(HttpStatus.SC_CONFLICT, deleteModules().getResponseCode());
    verifyAll(ONLY_MODULE1, ONLY_1_DEPENDENCIES);

    // drop the instance of type1, now delete of module1 should work
    instanceService.drop(NamespaceId.DEFAULT.dataset("instance1"));
    Assert.assertEquals(HttpStatus.SC_OK, deleteModules().getResponseCode());
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, getMissingType("datasetType1").getResponseCode());
    verifyAll(NO_MODULES, NO_DEPENDENCIES);
  }

  // Tests that a module can be deployed from a jar that is embedded in a bundle jar. This verifies class loading.
  @Test
  public void testBundledJarModule() throws Exception {
    // Get jar of TestModule1
    Location module1Jar = createModuleJar(TestModule1.class);
    // Create bundle jar with TestModule2 and TestModule1 inside it, request for deploy is made for Module1.
    Assert.assertEquals(200, deployModuleBundled("module1", TestModule1.class.getName(),
                                                 TestModule2.class, module1Jar));
    Assert.assertEquals(HttpStatus.SC_OK, deleteModules().getResponseCode());
    List<DatasetModuleMeta> modules = getModules().getResponseObject();
    Assert.assertEquals(0, modules.size());
  }

  @Test
  public void testNotFound() throws Exception {
    NamespaceId nonExistent = new NamespaceId("nonExistent");
    HttpResponse response = makeModulesRequest(nonExistent);
    assertNamespaceNotFound(response, nonExistent);

    response = makeTypesRequest(nonExistent);
    assertNamespaceNotFound(response, nonExistent);

    DatasetModuleId datasetModule = nonExistent.datasetModule("module");
    response = makeModuleInfoRequest(datasetModule);
    assertNamespaceNotFound(response, nonExistent);

    DatasetTypeId datasetType = nonExistent.datasetType("type");
    response = makeTypeInfoRequest(datasetType);
    assertNamespaceNotFound(response, nonExistent);

    response = deployModule(datasetModule, TestModule1.class);
    assertNamespaceNotFound(response, nonExistent);

    response = deleteModule(datasetModule);
    assertNamespaceNotFound(response, nonExistent);

    response = deleteModules(nonExistent);
    assertNamespaceNotFound(response, nonExistent);
  }

  private void verifyAll(Set<DatasetModuleMeta> expectedModules,
                         Map<String, List<String>> typeDependencies) throws IOException {
    Set<DatasetModuleMeta> actualModules = new HashSet<>(getModules().getResponseObject());
    Assert.assertEquals(expectedModules, actualModules);
    for (DatasetModuleMeta expectedModule : expectedModules) {
      ObjectResponse<DatasetModuleMeta> response = getModule(expectedModule.getName());
      Assert.assertEquals(HttpStatus.SC_OK, response.getResponseCode());
      Assert.assertEquals(expectedModule, response.getResponseObject());
      for (String type : expectedModule.getTypes()) {
        ObjectResponse<DatasetTypeMeta> typeResponse = getType(type);
        Assert.assertEquals(HttpStatus.SC_OK, typeResponse.getResponseCode());
        verify(typeResponse.getResponseObject(), type, typeDependencies.get(type));
      }
    }
    List<DatasetTypeMeta> actualTypes = getTypes().getResponseObject();
    Assert.assertEquals(actualTypes.size(), typeDependencies.size());
    Assert.assertTrue(Iterables.elementsEqual(
      typeDependencies.keySet(), Iterables.transform(actualTypes, new Function<DatasetTypeMeta, String>() {
        @Nullable
        @Override
        public String apply(@Nullable DatasetTypeMeta input) {
          return input == null ? null : input.getName();
        }
      })));
    for (DatasetTypeMeta typeMeta : actualTypes) {
      verify(typeMeta, typeMeta.getName(), typeDependencies.get(typeMeta.getName()));
    }
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
    // note: we know it is local
    Assert.assertNotNull(moduleMeta.getJarLocation());
    Assert.assertTrue(new File(moduleMeta.getJarLocation()).exists());
  }

  private ObjectResponse<List<DatasetTypeMeta>> getTypes() throws IOException {
    return getTypes(NamespaceId.DEFAULT);
  }

  private ObjectResponse<List<DatasetTypeMeta>> getTypes(NamespaceId namespaceId) throws IOException {
    return ObjectResponse.fromJsonBody(makeTypesRequest(namespaceId),
                                       new TypeToken<List<DatasetTypeMeta>>() { }.getType());
  }

  private HttpResponse makeTypesRequest(NamespaceId namespaceId) throws IOException {
    HttpRequest request = HttpRequest.get(getUrl(namespaceId.getEntityName(), "/data/types")).build();
    return HttpRequests.execute(request);
  }

  private ObjectResponse<DatasetModuleMeta> getModule(String moduleName) throws IOException {
    return getModule(NamespaceId.DEFAULT.datasetModule(moduleName));
  }

  private ObjectResponse<DatasetModuleMeta> getModule(DatasetModuleId module) throws IOException {
    return ObjectResponse.fromJsonBody(makeModuleInfoRequest(module), DatasetModuleMeta.class);
  }

  private HttpResponse makeModuleInfoRequest(DatasetModuleId module) throws IOException {
    HttpRequest request = HttpRequest.get(getUrl(module.getNamespace(),
                                                 "/data/modules/" + module.getEntityName())).build();
    return HttpRequests.execute(request);
  }

  private ObjectResponse<DatasetTypeMeta> getType(String typeName) throws IOException {
    return getType(NamespaceId.DEFAULT.datasetType(typeName));
  }

  private HttpResponse getMissingType(String typeName) throws IOException {
    return makeTypeInfoRequest(NamespaceId.DEFAULT.datasetType(typeName));
  }

  private ObjectResponse<DatasetTypeMeta> getType(DatasetTypeId datasetType) throws IOException {
    return ObjectResponse.fromJsonBody(makeTypeInfoRequest(datasetType), DatasetTypeMeta.class);
  }

  private HttpResponse makeTypeInfoRequest(DatasetTypeId datasetType) throws IOException {
    HttpRequest request = HttpRequest.get(
      getUrl(datasetType.getNamespace(), "/data/types/" + datasetType.getEntityName())).build();
    return HttpRequests.execute(request);
  }
}
