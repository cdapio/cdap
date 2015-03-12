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

package co.cask.cdap.api.dataset;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for building a DatsetSpecification.
 */
public class DatasetSpecificationTest {

  @Test
  public void testSimpleSpec() {
    String name = "name";
    String type = "type";
    String propKey = "prop1";
    String propVal = "val1";
    DatasetSpecification spec = DatasetSpecification.builder(name, type)
      .property(propKey, propVal)
      .build();
    Assert.assertEquals(name, spec.getName());
    Assert.assertEquals(type, spec.getType());
    Assert.assertEquals(propVal, spec.getProperty(propKey));
    Assert.assertEquals(1, spec.getProperties().size());
    Assert.assertTrue(spec.getSpecifications().isEmpty());
  }

  @Test
  public void testNamespacing() {
    DatasetSpecification innerSpec = DatasetSpecification.builder("inner", "table")
      .build();
    DatasetSpecification outerSpec = DatasetSpecification.builder("kv", "kvtable")
      .datasets(innerSpec)
      .build();
    Assert.assertEquals("kv", outerSpec.getName());
    Assert.assertEquals("kvtable", outerSpec.getType());

    DatasetSpecification actualInner = outerSpec.getSpecification("inner");
    Assert.assertEquals("kv.inner", actualInner.getName());
    Assert.assertEquals("table", actualInner.getType());
    Assert.assertTrue(actualInner.getSpecifications().isEmpty());
  }

  @Test
  public void testParent() {
    DatasetSpecification firstSpec = DatasetSpecification.builder("inner", "table")
      .build();
    DatasetSpecification secondSpec = DatasetSpecification.builder("kv", "kvtable")
      .datasets(firstSpec)
      .build();
    DatasetSpecification objSpec1 = DatasetSpecification.builder("obj", "objects")
      .datasets(secondSpec)
      .build();
    DatasetSpecification objSpec2 = DatasetSpecification.builder("objt", "objects")
      .datasets(secondSpec)
      .build();
    DatasetSpecification fourthSpec = DatasetSpecification.builder("history", "store")
      .datasets(objSpec1, objSpec2)
      .build();

    Assert.assertTrue(fourthSpec.isParent("history.obj.kv.inner"));
    Assert.assertTrue(objSpec2.isParent("objt.kv.inner"));
    Assert.assertTrue(fourthSpec.isParent("history.objt.kv.inner"));
    Assert.assertFalse(objSpec1.isParent("objt.kv.inner"));
    Assert.assertFalse(fourthSpec.isParent("history.obj.kv.outer"));
    Assert.assertFalse(fourthSpec.isParent("history.obj.kv.inner.outer"));
    Assert.assertFalse(fourthSpec.isParent("history.obj.kv.inner.inner"));
    Assert.assertFalse(fourthSpec.isParent("obj.kv.inner"));
  }
}
