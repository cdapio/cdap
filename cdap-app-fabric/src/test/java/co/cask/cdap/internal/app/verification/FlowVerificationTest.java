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

package co.cask.cdap.internal.app.verification;

import co.cask.cdap.WebCrawlApp;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.app.verification.VerifyResult;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.deploy.Specifications;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.proto.id.ApplicationId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Test all possible ways a Flow verification can fail.
 */
public class FlowVerificationTest {

  @Test
  public void testFlowWithMoreOutputThanWhatInputCanHandle() throws Exception {
    ApplicationSpecification appSpec = Specifications.from(new WebCrawlApp());
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    FlowVerification flowSpec = new FlowVerification();
    for (Map.Entry<String, FlowSpecification> entry : newSpec.getFlows().entrySet()) {
      VerifyResult result = flowSpec.verify(new ApplicationId("test", newSpec.getName()), entry.getValue());
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
    ApplicationSpecification appSpec = Specifications.from(new WebCrawlApp());
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    FlowVerification flowSpec = new FlowVerification();
    for (Map.Entry<String, FlowSpecification> entry : newSpec.getFlows().entrySet()) {
      VerifyResult result = flowSpec.verify(new ApplicationId("test", newSpec.getName()), entry.getValue());
      Assert.assertTrue(result.getStatus() == VerifyResult.Status.SUCCESS);
    }
  }


  /**
   *
   */
  public static class NoConsumerApp extends AbstractApplication {
    @Override
    public void configure() {
      setName("NoConsumerApp");
      setDescription("No consumer app");
      addStream(new Stream("text"));
      addFlow(new NoConsumerFlow());
    }

    /**
     *
     */
    public static class NoConsumerFlow extends AbstractFlow {

      @Override
      protected void configure() {
        setName("NoConsumerFlow");
        setDescription("No consumer flow");
        addFlowlet("s1", new SourceFlowlet());
        addFlowlet("s2", new SourceFlowlet());
        addFlowlet("dest", new DestFlowlet());
        connectStream("text", "s1");
        connectStream("text", "s2");
        connect("s1", "dest");
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
    ApplicationSpecification appSpec = Specifications.from(new NoConsumerApp());
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    FlowVerification flowVerifier = new FlowVerification();
    for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
      VerifyResult result = flowVerifier.verify(new ApplicationId("test", newSpec.getName()), flowSpec);
      Assert.assertTrue(result.getStatus() == VerifyResult.Status.FAILED);
    }
  }

}
