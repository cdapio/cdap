/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.security.auth.context;

import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import java.util.Properties;

/**
 * A dummy {@link AuthenticationContext} to be used in tests.
 */
public class AuthenticationTestContext implements AuthenticationContext {

  private static final String PRINCIPAL_NAME = "user.name";
  private static final String PRINCIPAL_CREDENTIAL_TYPE = "user.credential.type";
  private static final String PRINCIPAL_CREDENTIAL_VALUE = "user.credential.value";

  @Override
  public Principal getPrincipal() {
    Properties properties = System.getProperties();
    String credentialValue = properties.getProperty(PRINCIPAL_CREDENTIAL_VALUE);
    String credentialTypeStr = properties.getProperty(PRINCIPAL_CREDENTIAL_TYPE);
    Credential credential = null;
    if (credentialValue != null && credentialTypeStr != null) {
      Credential.CredentialType credentialType = Credential.CredentialType
          .valueOf(credentialTypeStr);
      credential = new Credential(credentialValue, credentialType);
    }
    return new Principal(System.getProperty(PRINCIPAL_NAME), Principal.PrincipalType.USER,
        credential);
  }

  /**
   * Sets the principal for this test authentication context.
   *
   * @param principal The principal to act as
   */
  public static void actAsPrincipal(Principal principal) {
    System.setProperty(PRINCIPAL_NAME, principal.getName());
    Credential credential = principal.getFullCredential();
    if (credential != null) {
      System.setProperty(PRINCIPAL_CREDENTIAL_TYPE, credential.getType().name());
      System.setProperty(PRINCIPAL_CREDENTIAL_VALUE, credential.getValue());
    }
  }
}
