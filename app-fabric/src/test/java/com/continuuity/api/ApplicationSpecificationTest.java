package com.continuuity.api;


import com.continuuity.ResourceApp;
import com.continuuity.WordCountApp;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.UnsupportedTypeException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ApplicationSpecificationTest {

  @Test
  public void testConfigureApplication() throws NoSuchMethodException, UnsupportedTypeException {
    ApplicationSpecification appSpec = new WordCountApp().configure();

    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());

    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    Assert.assertEquals(1, newSpec.getDataSets().size());
    Assert.assertEquals(new ReflectionSchemaGenerator().generate(WordCountApp.MyRecord.class),
                          newSpec.getFlows().get("WordCountFlow").getFlowlets().get("Tokenizer")
                                 .getInputs().get("").iterator().next());
  }

  @Test
  public void testConfigureResourcesApplication() throws UnsupportedTypeException {

    ApplicationSpecification appSpec = new ResourceApp().configure();

    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());

    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    // check flow resources
    Assert.assertEquals(1, newSpec.getFlows().size());
    Assert.assertTrue(newSpec.getFlows().containsKey("ResourceFlow"));
    FlowSpecification flowSpec = newSpec.getFlows().get("ResourceFlow");

    Assert.assertEquals(2, flowSpec.getFlowlets().size());
    Assert.assertTrue(flowSpec.getFlowlets().containsKey("A"));
    Assert.assertTrue(flowSpec.getFlowlets().containsKey("B"));

    FlowletDefinition flowletA = flowSpec.getFlowlets().get("A");
    Assert.assertEquals(2, flowletA.getFlowletSpec().getResources().getVirtualCores());
    Assert.assertEquals(1024, flowletA.getFlowletSpec().getResources().getMemoryMB());
    FlowletDefinition flowletB = flowSpec.getFlowlets().get("B");
    Assert.assertEquals(5, flowletB.getFlowletSpec().getResources().getVirtualCores());
    Assert.assertEquals(2048, flowletB.getFlowletSpec().getResources().getMemoryMB());

    // check procedure resources
    Assert.assertEquals(1, newSpec.getProcedures().size());
    ProcedureSpecification procedureSpec = newSpec.getProcedures().values().iterator().next();
    Assert.assertEquals(3, procedureSpec.getResources().getVirtualCores());
    Assert.assertEquals(128, procedureSpec.getResources().getMemoryMB());

    // check mapred resources
    Assert.assertEquals(1, newSpec.getMapReduce().size());
    MapReduceSpecification mapredSpec = newSpec.getMapReduce().values().iterator().next();
    Assert.assertEquals(512, mapredSpec.getMapperMemoryMB());
    Assert.assertEquals(1024, mapredSpec.getReducerMemoryMB());
  }
}
