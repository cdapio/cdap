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

package io.cdap.cdap.etl.common;

import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.FakeSecureStore;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the oauthStaticAccessToken macro
 */
public class OAuthStaticAccessTokenMacroEvaluatorTest {
  private static final String NAMESPACE = NamespaceId.SYSTEM.getNamespace();
  private static final String PROVIDER = "test";
  private static final String CREDENTIAL_ID = "testcredential";
  private static final String KEY = "oauthaccesstoken-test-testcredential";

  @Test
  public void testSecureMacroEvaluation() {
    String accessToken = "somecontent";
    String keyContent = String.format("{\"accessToken\":\"%s\",\"redirectURI\":\"uri\"}", accessToken);

    SecureStore secureStore = FakeSecureStore.builder()
      .putValue(NAMESPACE, KEY, keyContent)
      .build();

    MacroEvaluator macroEvaluator = new OAuthStaticAccessTokenMacroEvaluator(secureStore);
    String evaluatedAccessToken = macroEvaluator.evaluate(
        OAuthStaticAccessTokenMacroEvaluator.FUNCTION_NAME,
        PROVIDER,
        CREDENTIAL_ID);
    // assert tokens are the same
    Assert.assertEquals(accessToken, evaluatedAccessToken);
  }
}
