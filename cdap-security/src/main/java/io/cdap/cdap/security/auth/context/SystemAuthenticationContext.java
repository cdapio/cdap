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

import com.google.inject.Inject;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;

/**
 * Authentication context for master services which utilize internal authentication.
 */
public class SystemAuthenticationContext implements AuthenticationContext {
  public static final String SYSTEM_IDENTITY = "system";
  // Default expiration time of 5 minutes.
  private static final long DEFAULT_EXPIRATION = 300000;

  private final AccessTokenCodec accessTokenCodec;
  private final TokenManager tokenManager;

  @Inject
  SystemAuthenticationContext(AccessTokenCodec accessTokenCodec, TokenManager tokenManager) {
    this.accessTokenCodec = accessTokenCodec;
    this.tokenManager = tokenManager;
  }

  @Override
  public Principal getPrincipal() {
    // By default, assume the principal comes from a user request and handle accordingly using SecurityRequestContext.
    String userId = SecurityRequestContext.getUserId();
    Credential userCredential = SecurityRequestContext.getUserCredential();
    if (userId != null) {
      return new Principal(userId, Principal.PrincipalType.USER, userCredential);
    }

    // Use internal identity if user ID is null.
    // If user ID is null, the service is not handling a user request, so we assume it is an internal request.
    long currentTimestamp = System.currentTimeMillis();
    UserIdentity identity = new UserIdentity(SYSTEM_IDENTITY, Collections.emptyList(), currentTimestamp,
                                             currentTimestamp + DEFAULT_EXPIRATION);
    AccessToken accessToken = tokenManager.signIdentifier(identity);
    String encodedAccessToken;
    try {
      encodedAccessToken = Base64.getEncoder().encodeToString(accessTokenCodec.encode(accessToken));
      Credential credential = new Credential(encodedAccessToken, Credential.CredentialType.INTERNAL);
      return new Principal(SYSTEM_IDENTITY, Principal.PrincipalType.USER, credential);
    } catch (IOException e) {
      throw new RuntimeException("Unexpected failure while creating internal system identity", e);
    }
  }
}
