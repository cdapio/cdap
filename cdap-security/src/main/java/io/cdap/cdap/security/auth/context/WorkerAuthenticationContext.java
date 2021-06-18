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

/**
 * Authentication context for workers.
 */
public class WorkerAuthenticationContext implements AuthenticationContext {
  private static final String WORKER_USER_ID = "worker";
  private static final String PLACEHOLDER_CREDENTIAL = "placeholder";
  /**
   * Currently only returns a hardcoded set of user ID and credentials to get around the required auth limitation.
   * TODO CDAP-17772: Implement proper authentication context for workers.
   *
   * @return A placeholder principal for workers.
   */
  @Override
  public Principal getPrincipal() {
    return new Principal(WORKER_USER_ID, Principal.PrincipalType.USER,
                         new Credential(PLACEHOLDER_CREDENTIAL, Credential.CredentialType.INTERNAL));
  }
}
