/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.common.lang.jar.JarFinder;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.ObjectResponse;
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
 * Unit-test for {@link DatasetTypeHandlerV2}
 */
public class DatasetTypeHandlerV2Test extends DatasetServiceTestBase {

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
    HttpRequest request = HttpRequest.get(getUrl("/data/types")).build();
    return ObjectResponse.fromJsonBody(HttpRequests.execute(request),
                                       new TypeToken<List<DatasetTypeMeta>>() { }.getType());
  }

  private ObjectResponse<DatasetModuleMeta> getModule(String moduleName) throws IOException {
    HttpRequest request = HttpRequest.get(getUrl("/data/modules/" + moduleName)).build();
    return ObjectResponse.fromJsonBody(HttpRequests.execute(request), DatasetModuleMeta.class);
  }

  private ObjectResponse<DatasetTypeMeta> getType(String typeName) throws IOException {
    HttpRequest request = HttpRequest.get(getUrl("/data/types/" + typeName)).build();
    return ObjectResponse.fromJsonBody(HttpRequests.execute(request), DatasetTypeMeta.class);
  }

  @Test
  public void testBundledJarModule() throws Exception {
    //Get jar of TestModule1
    String module1Jar = JarFinder.getJar(TestModule1.class);
    // Create bundle jar with TestModule2 and TestModule1 inside it, request for deploy is made for Module1.
    Assert.assertEquals(200, deployModuleBundled("module1", TestModule1.class.getName(),
                                                 TestModule2.class, new File(module1Jar)));
    Assert.assertEquals(HttpStatus.SC_OK, deleteModules());
    List<DatasetModuleMeta> modules = getModules().getResponseObject();
    Assert.assertEquals(0, modules.size());
  }
}
