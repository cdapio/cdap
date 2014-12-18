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
}
