/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.verification;

import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.app.verification.VerifyResult;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.proto.id.ApplicationId;
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
  public void testGoodApplication() {
    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create();
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));
    ApplicationVerification app = new ApplicationVerification();
    VerifyResult result = app.verify(new ApplicationId("test", newSpec.getName()), newSpec);
    Assert.assertSame(result.getMessage(), result.getStatus(), VerifyResult.Status.SUCCESS);
  }

}
