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

import static io.cdap.cdap.common.http.HttpHeaderNames.WORKER_DECRYPTION_HDR;

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
  private final String expectedExemptionType;
  private final String testName;

  public EncryptionExemptionHookTest(String uri, String expectedExemptionType, String testName) {
    this.uri = uri;
    this.expectedExemptionType = expectedExemptionType;
    this.testName = testName;
  }

  // Define the data set for the tests.
  @Parameters(name = "{2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {"/v3Internal/namespaces/default/credentials/workloadIdentity/provision"
            + "?scopes=https://www.test.com/auth/test-platform",
            WorkerDecryptionScope.ANY_WORKER.name(), "CredentialsWithQueryParamsExempt"},
        {"/v3Internal/namespaces/default/credentials/workloadIdentity/provision",
            WorkerDecryptionScope.ANY_WORKER.name(), "CredentialsWithoutQueryParamsExempt"},
        {"/v3/namespaces/system/apps/pipeline/services/studio/methods"
            + "/v1/contexts/default/connections/testing", WorkerDecryptionScope.ANY_WORKER.name(),
            "ConnectionValidationExempt"},
        {"/v3/namespaces/system/apps/pipeline/services/studio/methods"
            + "/v1/oauth/provider/provider/credential/REUSE_PROV_FALSE",
            WorkerDecryptionScope.ANY_WORKER.name(), "OAuthMacroEvaluatorExempt"},

        {"/v3Internal/namespaces/system/artifacts/cdap-data-pipeline/versions?"
            + "lower=6.12.0-SNAPSHOT&upper=6.12.0-SNAPSHOT&limit=1&order=DE",
            WorkerDecryptionScope.PREVIEW_RUNNER.name(), "ArtifactsWithVersionParams"},
        {"/v3Internal/namespaces/system/artifacts/MyApp/versions",
            WorkerDecryptionScope.PREVIEW_RUNNER.name(), "ArtifactsVersionlessExempt"},
        {"/v3Internal/namespaces/default/apps/e99c8132-8edd-11f0-9607-36a08ae62395/"
            + "workflows/DataPipelineWorkflow/preferences?resolved=true",
            WorkerDecryptionScope.PREVIEW_RUNNER.name(), "WorkflowPreferencesExempt"},
        {"/v3/namespaces/user/data/datasets/MyDatasetName",
            WorkerDecryptionScope.PREVIEW_RUNNER.name(), "DataDatasetsExempt"},
        {"/v1/namespaces/system/topics/preview/publish",
            WorkerDecryptionScope.PREVIEW_RUNNER.name(),
            "SystemTopicPublishExempt"},

        {"/v3Internal/namespaces/default/preferences?key=value",
            WorkerDecryptionScope.TASK_WORKER.name(), "GenericPreferencesExempt"},
        {"/v3/namespaces/default/securekeys/personal-token",
            WorkerDecryptionScope.TASK_WORKER.name(),
            "SecureKeysExempt"},

        {"testing", WorkerDecryptionScope.NONE.name(), "NonExemptGeneric"},
        {"/v3/system/services/status", WorkerDecryptionScope.NONE.name(), "NonExemptV3Status"}
    });
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

    String actualHeader = request.headers().get(WORKER_DECRYPTION_HDR);

    String message = String.format(
        "Test case '%s' failed for URI: %s. Expected Exemption Type: %s, Actual: %s.",
        this.testName, this.uri, this.expectedExemptionType, actualHeader);

    Assert.assertEquals(message, this.expectedExemptionType, actualHeader);
  }
}
