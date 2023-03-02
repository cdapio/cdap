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

package io.cdap.cdap.security.auth.context;

import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;

/**
 * Authentication context for workers.
 */
public class WorkerAuthenticationContext implements AuthenticationContext {

  private static final Principal EMPTY_PRINCIPAL = new Principal("empty-worker-user-id",
      Principal.PrincipalType.USER,
      new Credential("empty-worker-user-credential",
          Credential.CredentialType.INTERNAL));

  /**
   * Return {@link Principal} associated with current request stored in {@link
   * SecurityRequestContext}. Typically, there is always a {@link Principal} as worker normally
   * performs some operations on behalf of end user, thus the {@link Principal} should capture the
   * credential of end user. But when there is none, use placeholder values to construct the {@link
   * Principal}.
   */
  @Override
  public Principal getPrincipal() {
    // By default, assume the principal comes from a user request and handle accordingly using SecurityRequestContext.
    String userId = SecurityRequestContext.getUserId();
    Credential userCredential = SecurityRequestContext.getUserCredential();
    if (userId != null && userCredential != null) {
      return new Principal(userId, Principal.PrincipalType.USER, userCredential);
    }
    return EMPTY_PRINCIPAL;
  }
}
