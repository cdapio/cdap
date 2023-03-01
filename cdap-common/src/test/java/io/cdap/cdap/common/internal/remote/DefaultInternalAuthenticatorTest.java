/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link DefaultInternalAuthenticator}.
 */
public class DefaultInternalAuthenticatorTest {
  @Test
  public void testProperHeadersSet() {
    Map<String, String> stringMap = new HashMap<>();

    // Set authentication context principal.
    String expectedName = "somebody";
    String expectedCredValue = "credential";
    Credential.CredentialType expectedCredType = Credential.CredentialType.EXTERNAL;
    Credential credential = new Credential(expectedCredValue, expectedCredType);
    Principal expectedPrincipal = new Principal(expectedName, Principal.PrincipalType.USER, credential);

    DefaultInternalAuthenticator defaultInternalAuthenticator =
      new DefaultInternalAuthenticator(new TestAuthenticationContext(expectedPrincipal));

    defaultInternalAuthenticator.applyInternalAuthenticationHeaders(stringMap::put);

    // Verify return values
    Assert.assertEquals(expectedName, stringMap.get(Constants.Security.Headers.USER_ID));
    Assert.assertEquals(String.format("%s %s", expectedCredType.getQualifiedName(), expectedCredValue),
                        stringMap.get(Constants.Security.Headers.RUNTIME_TOKEN));
  }

  private static class TestAuthenticationContext implements AuthenticationContext {
    private final Principal principal;

    private TestAuthenticationContext(Principal principal) {
      this.principal = principal;
    }

    @Override
    public Principal getPrincipal() {
      return principal;
    }
  }
}
