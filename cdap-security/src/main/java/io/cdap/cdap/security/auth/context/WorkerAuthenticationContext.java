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
  private static final String WORKER_STARTUP_ID = "worker-startup";
  private static final String WORKER_STARTUP_CREDENTIAL = "worker-startup-creds";

  /**
   * Returns the user identity and credential of the entity making the request to run a task.
   * @return A principal for workers based on who is making the request to run a task.
   */
  @Override
  public Principal getPrincipal() {
    String userID = SecurityRequestContext.getUserId();
    Credential userCredential = SecurityRequestContext.getUserCredential();
    if (userID == null) {
      // During task pod startup, there may not be a user ID and credential yet. However, the task pods still need
      // to connect to the file localizer endpoint to retrieve configuration files.
      userID = WORKER_STARTUP_ID;
      userCredential = new Credential(WORKER_STARTUP_CREDENTIAL, Credential.CredentialType.INTERNAL);
    }
    return new Principal(userID, Principal.PrincipalType.USER, userCredential);
  }
}
