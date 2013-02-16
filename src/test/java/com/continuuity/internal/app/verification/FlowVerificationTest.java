/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.verification;

import com.continuuity.WebCrawlApp;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.io.SimpleQueueSpecificationGeneratorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Test all possible ways a Flow verification can fail.
 */
public class FlowVerificationTest {

  @Test
  public void testFlowWithMoreOutputThanWhatInputCanHandle() throws Exception {
    ApplicationSpecification appSpec = new WebCrawlApp().configure();
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(SimpleQueueSpecificationGeneratorFactory.create());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    FlowVerification flowSpec = new FlowVerification();
    for(Map.Entry<String, FlowSpecification> entry : newSpec.getFlows().entrySet()) {
      VerifyResult result = flowSpec.verify(entry.getValue());
      // This is the flow that has Tokenizer flowlet that defines one more output called "mylist"
      // that is not connected to any input to flowlet CountByField.
      if(entry.getValue().getName().equals("WordCountFlow")) {
        Assert.assertTrue(result.getStatus() == VerifyResult.Status.FAILED);
      } else {
        Assert.assertTrue(result.getStatus() == VerifyResult.Status.SUCCESS);
      }
    }
  }

  @Test
  public void testValidFlow() throws Exception {
    ApplicationSpecification appSpec = new WebCrawlApp().configure();
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(SimpleQueueSpecificationGeneratorFactory.create());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    FlowVerification flowSpec = new FlowVerification();
    for(Map.Entry<String, FlowSpecification> entry : newSpec.getFlows().entrySet()) {
      VerifyResult result = flowSpec.verify(entry.getValue());
      Assert.assertTrue(result.getStatus() == VerifyResult.Status.SUCCESS);
    }
  }

}
