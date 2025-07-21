/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.proto.sourcecontrol;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import org.junit.Test;

public class AuthConfigTest {

  @Test
  public void testHttpAccessTokenWithBitbucketServer_shouldPass() {
    PatConfig patConfig = new PatConfig("test-name", "");
    AuthConfig authConfig = new AuthConfig(AuthType.HTTP_ACCESS_TOKEN, patConfig);

    Collection<RepositoryValidationFailure> failures = authConfig.validate(
        Provider.BITBUCKET_SERVER);

    assertTrue("Expected no validation errors", failures.isEmpty());
  }

  @Test
  public void testHttpAccessTokenWithNonBitbucketProvider_shouldFailValidation() {
    PatConfig patConfig = new PatConfig("test-password", "x-token-auth");
    AuthConfig authConfig = new AuthConfig(AuthType.HTTP_ACCESS_TOKEN, patConfig);

    Collection<RepositoryValidationFailure> failures = authConfig.validate(Provider.GITHUB);

    assertFalse("Expected validation failures", failures.isEmpty());

    String expectedMessage = "HTTP access token must only be used with bitbucket server.";
    boolean hasExpectedMessage = false;
    for (RepositoryValidationFailure failure : failures) {
      if (expectedMessage.equals(failure.getMessage())) {
        hasExpectedMessage = true;
        break;
      }
    }

    assertTrue("Expected specific validation message: "
        + expectedMessage, hasExpectedMessage);
  }
}
