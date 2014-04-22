/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.verification;

import com.continuuity.WebCrawlApp;
import com.continuuity.api.Application;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.app.Specifications;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Test all possible ways a Flow verification can fail.
 */
public class FlowVerificationTest {

  @Test
  public void testFlowWithMoreOutputThanWhatInputCanHandle() throws Exception {
    ApplicationSpecification appSpec = Specifications.from(new WebCrawlApp().configure());
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    FlowVerification flowSpec = new FlowVerification();
    for (Map.Entry<String, FlowSpecification> entry : newSpec.getFlows().entrySet()) {
      VerifyResult result = flowSpec.verify(Id.Application.from("test", newSpec.getName()), entry.getValue());
      // This is the flow that has Tokenizer flowlet that defines one more output called "mylist"
      // that is not connected to any input to flowlet CountByField.
      if (entry.getValue().getName().equals("WordCountFlow")) {
        Assert.assertTrue(result.getStatus() == VerifyResult.Status.FAILED);
      } else {
        Assert.assertTrue(result.getStatus() == VerifyResult.Status.SUCCESS);
      }
    }
  }

  @Test
  public void testValidFlow() throws Exception {
    ApplicationSpecification appSpec = Specifications.from(new WebCrawlApp().configure());
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    FlowVerification flowSpec = new FlowVerification();
    for (Map.Entry<String, FlowSpecification> entry : newSpec.getFlows().entrySet()) {
      VerifyResult result = flowSpec.verify(Id.Application.from("test", newSpec.getName()), entry.getValue());
      Assert.assertTrue(result.getStatus() == VerifyResult.Status.SUCCESS);
    }
  }


  /**
   *
   */
  public static class NoConsumerApp implements Application {

    @Override
    public com.continuuity.api.ApplicationSpecification configure() {
      return com.continuuity.api.ApplicationSpecification.Builder.with()
        .setName("NoConsumerApp")
        .setDescription("No consumer app")
        .withStreams().add(new Stream("text"))
        .noDataSet()
        .withFlows().add(new NoConsumerFlow())
        .noProcedure()
        .noMapReduce()
        .noWorkflow()
        .build();
    }

    /**
     *
     */
    public static class NoConsumerFlow implements Flow {

      @Override
      public FlowSpecification configure() {
        return FlowSpecification.Builder.with()
          .setName("NoConsumerFlow")
          .setDescription("No consumer flow")
          .withFlowlets()
          .add("s1", new SourceFlowlet())
          .add("s2", new SourceFlowlet())
          .add("dest", new DestFlowlet())
          .connect()
          .fromStream("text").to("s1")
          .fromStream("text").to("s2")
          .from("s1").to("dest")
          .build();
      }
    }

    /**
     *
     */
    public static class SourceFlowlet extends AbstractFlowlet {
      private OutputEmitter<String> output;

      public void process(StreamEvent event) {
        output.emit(getContext().getName());
      }
    }

    /**
     *
     */
    public static class DestFlowlet extends AbstractFlowlet {
      public void process(String str) {
        System.out.println(str);
      }
    }
  }

  /**
   * This test that verification of flow connections
   */
  @Test
  public void testFlowMissingConnection() throws Exception {
    ApplicationSpecification appSpec = Specifications.from(new NoConsumerApp().configure());
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    FlowVerification flowVerifier = new FlowVerification();
    for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
      VerifyResult result = flowVerifier.verify(Id.Application.from("test", newSpec.getName()), flowSpec);
      Assert.assertTrue(result.getStatus() == VerifyResult.Status.FAILED);
    }
  }

}
