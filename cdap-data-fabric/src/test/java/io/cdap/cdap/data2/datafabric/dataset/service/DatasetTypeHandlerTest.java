/*
 * Copyright © 2014-2018 Cask Data, Inc.
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.proto.DatasetInstanceConfiguration;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.DatasetTypeId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit-test for {@link DatasetTypeHandler}
 */
public class DatasetTypeHandlerTest extends DatasetServiceTestBase {

  private static final Set<DatasetModuleMeta> NO_MODULES = Collections.emptySet();
  private static final Map<String, List<String>> NO_DEPENDENCIES = Collections.emptyMap();

  @BeforeClass
  public static void setup() throws Exception {
    DatasetServiceTestBase.initialize();
  }

  @Test
  public void testSystemModules() throws Exception {
    // the modules that are available in the classpath are registered in the system namespace
    Map<String, DatasetModuleMeta> systemModules = new HashMap<>();
    for (DatasetModuleMeta moduleMeta : getModules(NamespaceId.SYSTEM).getResponseObject()) {
      systemModules.put(moduleMeta.getName(), moduleMeta);
    }
    verify(systemModules.get("module1"), "module1", TestModule1.class,
           ImmutableList.of("datasetType1"), Collections.emptyList(), ImmutableList.of("module2"));
    verify(systemModules.get("module2"), "module2", TestModule2.class,
           ImmutableList.of("datasetType2"), ImmutableList.of("module1"), Collections.emptyList());

    // their types can be retrieved, with the modules in the order they have to be loaded
    ObjectResponse<DatasetTypeMeta> typeResponse = getType(NamespaceId.SYSTEM.datasetType("datasetType2"));
    Assert.assertEquals(HttpStatus.SC_OK, typeResponse.getResponseCode());
    verify(typeResponse.getResponseObject(), "datasetType2", ImmutableList.of("module1", "module2"));

    // system modules cannot be deleted
    Assert.assertNotEquals(HttpStatus.SC_OK,
                           deleteModule(NamespaceId.SYSTEM.datasetModule("module2")).getResponseCode());
    Assert.assertNotEquals(HttpStatus.SC_OK, deleteModules(NamespaceId.SYSTEM).getResponseCode());
    Assert.assertEquals(systemModules.size(), getModules(NamespaceId.SYSTEM).getResponseObject().size());

    // no modules and types exist outside of the system namespace
    verifyAll(NO_MODULES, NO_DEPENDENCIES);
  }

  /**
   * Tests that on startup of the dataset type service, all existing modules of the system namespace are removed
   * before the modules available in the classpath are added.
   */
  @Test
  public void testSystemModulesReplacedOnStartup() throws Exception {
    int numSystemModules = getModules(NamespaceId.SYSTEM).getResponseObject().size();

    // write two modules with a jar location to the system namespace: one with a name that no system module has,
    // and one that replaces a system module
    Location leftoverJar = createModuleJar("leftover");
    writeModule(NamespaceId.SYSTEM,
                new DatasetModuleMeta("leftover", "io.cdap.cdap.test.NotInClasspathModule", leftoverJar.toURI(),
                                      ImmutableList.of("leftoverType"), Collections.emptyList()));
    writeModule(NamespaceId.SYSTEM,
                new DatasetModuleMeta("module1", "io.cdap.cdap.test.NotInClasspathModule",
                                      createModuleJar("module1").toURI(),
                                      ImmutableList.of("datasetType1"), Collections.emptyList(),
                                      ImmutableList.of("module2")));
    Assert.assertEquals(numSystemModules + 1, getModules(NamespaceId.SYSTEM).getResponseObject().size());
    Assert.assertEquals(HttpStatus.SC_OK,
                        getType(NamespaceId.SYSTEM.datasetType("leftoverType")).getResponseCode());

    // start another instance of the type service on the same store
    DefaultDatasetTypeService typeService = createTypeService();
    typeService.startAsync().awaitRunning();
    try {
      // the module that is not available in the classpath and its type are gone
      Map<String, DatasetModuleMeta> systemModules = new HashMap<>();
      for (DatasetModuleMeta moduleMeta : getModules(NamespaceId.SYSTEM).getResponseObject()) {
        Assert.assertNull(moduleMeta.getName(), moduleMeta.getJarLocationPath());
        systemModules.put(moduleMeta.getName(), moduleMeta);
      }
      Assert.assertEquals(numSystemModules, systemModules.size());
      Assert.assertFalse(systemModules.containsKey("leftover"));
      Assert.assertEquals(HttpStatus.SC_NOT_FOUND,
                          makeTypeInfoRequest(NamespaceId.SYSTEM.datasetType("leftoverType")).getResponseCode());

      // the system modules are the ones from the classpath again
      verify(systemModules.get("module1"), "module1", TestModule1.class,
             ImmutableList.of("datasetType1"), Collections.emptyList(), ImmutableList.of("module2"));
      verify(systemModules.get("module2"), "module2", TestModule2.class,
             ImmutableList.of("datasetType2"), ImmutableList.of("module1"), Collections.emptyList());
      for (DatasetTypeMeta typeMeta : getTypes(NamespaceId.SYSTEM).getResponseObject()) {
        for (DatasetModuleMeta moduleMeta : typeMeta.getModules()) {
          Assert.assertNull(typeMeta.getName(), moduleMeta.getJarLocationPath());
        }
      }

      // and their types can be used
      instanceService.create(NamespaceId.DEFAULT.getNamespace(), "instance2",
                             new DatasetInstanceConfiguration("datasetType2", new HashMap<>()));
      instanceService.drop(NamespaceId.DEFAULT.dataset("instance2"));
    } finally {
      typeService.stopAsync().awaitTerminated();
    }
  }

  @Test
  public void testAddModuleNotSupported() throws Exception {
    // there is no endpoint to add a dataset module
    HttpRequest request = HttpRequest.put(getUrl("/data/modules/module3"))
      .addHeader("X-Class-Name", TestModule1x.class.getName())
      .withBody("module content").build();
    Assert.assertEquals(HttpStatus.SC_METHOD_NOT_ALLOWED,
                        HttpRequests.execute(request, REQUEST_CONFIG).getResponseCode());
    verifyAll(NO_MODULES, NO_DEPENDENCIES);
  }

  @Test
  public void testListAndDeleteExistingModules() throws Exception {
    // nothing has been added, modules and types list is empty
    verifyAll(NO_MODULES, NO_DEPENDENCIES);

    // write two modules to the store, where the second one depends on the first one, and an instance that
    // uses a type of the second one
    Location jar1 = createModuleJar("existing1");
    Location jar2 = createModuleJar("existing2");
    DatasetModuleMeta module1Used =
      new DatasetModuleMeta("existing1", TestModule1x.class.getName(), jar1.toURI(),
                            ImmutableList.of("existingType1", "existingType1x"),
                            Collections.emptyList(), ImmutableList.of("existing2"));
    DatasetModuleMeta module1Unused =
      new DatasetModuleMeta("existing1", TestModule1x.class.getName(), jar1.toURI(),
                            ImmutableList.of("existingType1", "existingType1x"),
                            Collections.emptyList());
    DatasetModuleMeta module2 =
      new DatasetModuleMeta("existing2", TestModule2.class.getName(), jar2.toURI(),
                            ImmutableList.of("existingType2"),
                            ImmutableList.of("existing1"));
    writeModule(NamespaceId.DEFAULT, module1Used);
    writeModule(NamespaceId.DEFAULT, module2);
    writeInstance(NamespaceId.DEFAULT, DatasetSpecification.builder("instance2", "existingType2").build());

    Set<DatasetModuleMeta> bothModules = ImmutableSet.of(module1Used, module2);
    Map<String, List<String>> bothDependencies =
      ImmutableMap.of("existingType1", ImmutableList.of("existing1"),
                      "existingType1x", ImmutableList.of("existing1"),
                      "existingType2", ImmutableList.of("existing1", "existing2"));
    verifyAll(bothModules, bothDependencies);

    // cannot delete non-existent module
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, deleteModule("non-existing-module").getResponseCode());

    // cannot delete the first module since the second one depends on it, verify that nothing has been deleted
    Assert.assertEquals(HttpStatus.SC_CONFLICT, deleteModule("existing1").getResponseCode());
    verifyAll(bothModules, bothDependencies);

    // cannot delete the second module because there is an instance
    Assert.assertEquals(HttpStatus.SC_CONFLICT, deleteModule("existing2").getResponseCode());
    Assert.assertEquals(HttpStatus.SC_CONFLICT, deleteModules().getResponseCode());
    verifyAll(bothModules, bothDependencies);
    Assert.assertTrue(jar1.exists());
    Assert.assertTrue(jar2.exists());

    // drop the instance, now delete of the second module should work. It should be removed from usedBy list
    // everywhere and all its types should no longer be available
    instanceService.drop(NamespaceId.DEFAULT.dataset("instance2"));
    Assert.assertEquals(HttpStatus.SC_OK, deleteModule("existing2").getResponseCode());
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, getMissingType("existingType2").getResponseCode());
    verifyAll(ImmutableSet.of(module1Unused),
              ImmutableMap.of("existingType1", ImmutableList.of("existing1"),
                              "existingType1x", ImmutableList.of("existing1")));
    Assert.assertTrue(jar1.exists());
    Assert.assertFalse(jar2.exists());

    // cannot delete the second module again
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, deleteModule("existing2").getResponseCode());

    // delete all modules
    Assert.assertEquals(HttpStatus.SC_OK, deleteModules().getResponseCode());
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, getMissingType("existingType1").getResponseCode());
    verifyAll(NO_MODULES, NO_DEPENDENCIES);
    Assert.assertFalse(jar1.exists());
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
      typeDependencies.keySet(), actualTypes.stream().map(
        input -> input == null ? null : input.getName()).collect(Collectors.toList())));
    for (DatasetTypeMeta typeMeta : actualTypes) {
      verify(typeMeta, typeMeta.getName(), typeDependencies.get(typeMeta.getName()));
    }
  }

  private void verify(DatasetTypeMeta typeMeta, String typeName, List<String> modules) {
    Assert.assertEquals(typeName, typeMeta.getName());
    Assert.assertArrayEquals(modules.toArray(),
                             typeMeta.getModules().stream().map(
                               input -> input == null ? null : input.getName()).toArray());
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
    // note: the module is available in the classpath, hence it has no jar
    Assert.assertNull(moduleMeta.getJarLocationPath());
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
    return HttpRequests.execute(request, REQUEST_CONFIG);
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
    return HttpRequests.execute(request, REQUEST_CONFIG);
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
    return HttpRequests.execute(request, REQUEST_CONFIG);
  }
}
