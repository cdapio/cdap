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

package io.cdap.cdap.common.encryption;

import static io.cdap.cdap.common.http.HttpHeaderNames.TASK_WORKER_DECRYPTION_HDR;

import io.cdap.http.internal.HandlerInfo;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests for {@link EncryptionExemptionHook}.
 */
@RunWith(Parameterized.class)
public class EncryptionExemptionHookTest {

  private HandlerInfo handlerInfo;
  private EncryptionExemptionHook hook;

  // Parameters for the test
  private final String uri;
  private final boolean expectedDecryptionHeader;
  private final String testName;

  public EncryptionExemptionHookTest(String uri, boolean expectedDecryptionHeader,
      String testName) {
    this.uri = uri;
    this.expectedDecryptionHeader = expectedDecryptionHeader;
    this.testName = testName;
  }

  // Define the data set for the tests
  @Parameters(name = "{2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        // Successful (Exempt) Tests - Header should be FALSE.
        {"/v3/namespaces/default/securekeys/personal-token", false, "SecureKeysExempt"},
        {"/v3Internal/namespaces/default/credentials/workloadIdentity/provision"
            + "?scopes=https://www.test.com/auth/test-platform",
            false, "CredentialsWithQueryParamsExempt"},
        {"/v3Internal/namespaces/default/credentials/workloadIdentity/provision", false,
            "CredentialsWithoutQueryParamsExempt"},
        {"/v3/namespaces/system/apps/pipeline/services/studio/methods"
            + "/v1/contexts/default/connections/testing",
            false, "ConnectionValidationExempt"},
        {"/v3/namespaces/system/apps/pipeline/services/studio/methods"
            + "/v1/oauth/provider/provider/credential/REUSE_PROV_FALSE",
            false, "OAuthMacroEvaluatorExempt"},
        // Unsuccessful (Non-Exempt) Test - Header should be TRUE.
        {"testing", true, "NonExempt"}});
  }

  @Before
  public void setup() {
    handlerInfo = new HandlerInfo("test.handler", "testMethod");
    hook = new EncryptionExemptionHook();
  }

  @Test
  public void testPreCall() {
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, this.uri);

    hook.preCall(request, null, handlerInfo);

    String message = String.format("Test case '%s' failed for URI: %s. Expected header value: %s.",
        this.testName, this.uri, this.expectedDecryptionHeader);

    Assert.assertEquals(message, this.expectedDecryptionHeader, isDecryptionHeaderSet(request));
  }

  private boolean isDecryptionHeaderSet(HttpRequest request) {
    String headerValue = request.headers().get(TASK_WORKER_DECRYPTION_HDR);
    return Boolean.parseBoolean(headerValue);
  }
}
