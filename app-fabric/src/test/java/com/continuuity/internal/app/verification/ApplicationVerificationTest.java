/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.app.verification;

import com.continuuity.WebCrawlApp;
import com.continuuity.api.Application;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.app.Specifications;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.proto.Id;
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
