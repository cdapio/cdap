package com.continuuity.api;


import com.continuuity.WordCountApp;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.io.UnsupportedTypeException;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.app.ForwardingApplicationSpecification;
import com.continuuity.internal.app.ForwardingFlowSpecification;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;

/**
 *
 */
public class ApplicationSpecificationTest {

  @Test
  public void testConfigureApplication() throws NoSuchMethodException, UnsupportedTypeException {
    ApplicationSpecification appSpec = new WordCountApp().configure();

    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());

    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    final FlowSpecification flowOverride = new ForwardingFlowSpecification(newSpec.getFlows().get("WordCountFlow")) {
      @Override
      public String getName() {
        return "NewName";
      }
    };

    ApplicationSpecification override = new ForwardingApplicationSpecification(newSpec) {
      @Override
      public Map<String, FlowSpecification> getFlows() {
        Map<String, FlowSpecification> flows = Maps.newHashMap(super.getFlows());
        flows.remove("WordCountFlow");
        flows.put("NewName", flowOverride);
        return ImmutableMap.copyOf(flows);
      }
    };

    System.out.println(adapter.toJson(override));

//    Assert.assertEquals(1, newSpec.getDataSets().size());
//    Assert.assertEquals(new ReflectionSchemaGenerator().generate(WordCountApp.MyRecord.class),
//                          newSpec.getFlows().get("WordCountFlow").getFlowlets().get("Tokenizer")
//                                 .getInputs().get("").iterator().next());
  }
}
