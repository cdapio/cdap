package com.continuuity.api;

import com.continuuity.jetstream.api.AbstractGSFlowlet;
import com.continuuity.jetstream.api.GSSchema;
import com.continuuity.jetstream.api.PrimitiveType;
import com.continuuity.jetstream.gsflowlet.GSFlowletSpecification;
import com.continuuity.jetstream.internal.DefaultGSFlowletConfigurer;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tests the GSFlowletSpecification.
 */
public class GSFlowetSpecificationTest {

  @Test
  public void testBasicGSFlowlet() {
    AbstractGSFlowlet flowlet = new GSFlowletBasic();
    DefaultGSFlowletConfigurer configurer = new DefaultGSFlowletConfigurer(flowlet);
    flowlet.create(configurer);
    GSFlowletSpecification spec = configurer.createGSFlowletSpec();
    Assert.assertEquals(spec.getName(), "summation");
    Assert.assertEquals(spec.getDescription(), "sums up the input value over a timewindow");
    Assert.assertEquals(spec.getGdatInputSchema().size(), 1);
    Assert.assertTrue(spec.getGdatInputSchema().containsKey("intInput"));
    GSSchema schema = spec.getGdatInputSchema().get("intInput");
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
    AbstractGSFlowlet flowlet = new InvalidGSFlowlet();
    DefaultGSFlowletConfigurer configurer = new DefaultGSFlowletConfigurer(flowlet);
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
