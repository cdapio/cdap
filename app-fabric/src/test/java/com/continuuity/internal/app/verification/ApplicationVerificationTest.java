/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.verification;

import com.continuuity.WebCrawlApp;
import com.continuuity.api.Application;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.app.Specifications;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the verification of Application
 */
public class ApplicationVerificationTest {

  /**
   * Good test
   */
  @Test
  public void testGoodApplication() throws Exception {
    ApplicationSpecification appSpec = Specifications.from(new WebCrawlApp().configure());
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));
    ApplicationVerification app = new ApplicationVerification();
    VerifyResult result = app.verify(Id.Application.from("test", newSpec.getName()), newSpec);
    Assert.assertTrue(result.getMessage(), result.getStatus() == VerifyResult.Status.SUCCESS);
  }

  private static class ApplicationWithBadId implements Application {
    @Override
    public com.continuuity.api.ApplicationSpecification configure() {
      return com.continuuity.api.ApplicationSpecification.Builder.with()
        .setName("Bad App Name")
        .setDescription("Bad Application Name Test")
        .noStream()
        .noDataSet()
        .noFlow()
        .noProcedure()
        .noMapReduce()
        .noWorkflow()
        .build();
    }
  }

  /**
   * This test that verification of application id fails.
   */
  @Test
  public void testApplicationWithBadId() throws Exception {
    ApplicationSpecification appSpec =
      Specifications.from(new ApplicationWithBadId().configure());
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));
    ApplicationVerification app = new ApplicationVerification();
    VerifyResult result = app.verify(Id.Application.from("test", newSpec.getName()), newSpec);
    Assert.assertTrue(result.getMessage(), result.getStatus() == VerifyResult.Status.FAILED);
  }

}
