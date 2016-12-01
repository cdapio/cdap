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

package co.cask.cdap.internal.app.verification;

import co.cask.cdap.WebCrawlApp;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.app.verification.VerifyResult;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.deploy.Specifications;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.proto.id.ApplicationId;
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
    ApplicationSpecification appSpec = Specifications.from(new WebCrawlApp());
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));
    ApplicationVerification app = new ApplicationVerification();
    VerifyResult result = app.verify(new ApplicationId("test", newSpec.getName()), newSpec);
    Assert.assertTrue(result.getMessage(), result.getStatus() == VerifyResult.Status.SUCCESS);
  }

}
