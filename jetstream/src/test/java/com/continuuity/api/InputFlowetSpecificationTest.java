/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.api;

import com.continuuity.jetstream.api.AbstractInputFlowlet;
import com.continuuity.jetstream.api.StreamSchema;
import com.continuuity.jetstream.api.PrimitiveType;
import com.continuuity.jetstream.flowlet.InputFlowletSpecification;
import com.continuuity.jetstream.internal.DefaultInputFlowletConfigurer;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tests the InputFlowletSpecification.
 */
public class InputFlowetSpecificationTest {

  @Test
  public void testBasicFlowlet() {
    AbstractInputFlowlet flowlet = new InputFlowletBasic();
    DefaultInputFlowletConfigurer configurer = new DefaultInputFlowletConfigurer(flowlet);
    flowlet.create(configurer);
    InputFlowletSpecification spec = configurer.createInputFlowletSpec();
    Assert.assertEquals(spec.getName(), "summation");
    Assert.assertEquals(spec.getDescription(), "sums up the input value over a timewindow");
    Assert.assertEquals(spec.getGDATInputSchema().size(), 1);
    Assert.assertTrue(spec.getGDATInputSchema().containsKey("intInput"));
    StreamSchema schema = spec.getGDATInputSchema().get("intInput");
    Assert.assertEquals(schema.getIncreasingFields().size(), 1);
    Assert.assertTrue(schema.getIncreasingFields().contains("timestamp"));
    Assert.assertEquals(schema.getDecreasingFields().size(), 0);
    Assert.assertEquals(schema.getFieldNames().size(), 2);
    Assert.assertEquals(schema.getFieldNames().get("timestamp"), PrimitiveType.ULLONG);
    Assert.assertEquals(schema.getFieldNames().get("iStream"), PrimitiveType.UINT);
    LinkedHashMap<String, PrimitiveType> fields = schema.getFieldNames();
    //LinkedHashMap should preserve the order in which fields are inserted.
    for (Map.Entry<String, PrimitiveType> field : fields.entrySet()) {
      Assert.assertEquals(field.getKey(), "timestamp");
      break;
    }
    Assert.assertEquals(spec.getGSQL().size(), 1);
  }

  @Test
  public void testInvalidSchemaFlowlet() {
    AbstractInputFlowlet flowlet = new InvalidInputFlowlet();
    DefaultInputFlowletConfigurer configurer = new DefaultInputFlowletConfigurer(flowlet);
    int testValue = 0;
    try {
      flowlet.create(configurer);
    } catch (Exception e) {
      testValue = 1;
    } finally {
      Assert.assertEquals(testValue, 1);
    }
  }
}
