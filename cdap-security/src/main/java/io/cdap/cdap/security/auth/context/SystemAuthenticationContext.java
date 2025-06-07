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

import com.google.common.base.Throwables;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authentication context for master services which utilize internal authentication.
 */
public class SystemAuthenticationContext implements AuthenticationContext {

  private static final Logger LOG = LoggerFactory.getLogger(SystemAuthenticationContext.class);

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
    // Normally userID and userCredentials should be either null or non-null.
    // For non-null, they are either user or internal user credentials, so propagated as is.
    // For null, it means system originated requests, user and generate a credential as internal user.
    //
    // It is possible that userID is non-null while userCredential is null, this can happen when we want
    // to launch programs as a userID that is stored in program options' system args. As user credential
    // is currently not stored there, we cannot launch program as the targeted user, instead we run program
    // using system internal identity. We rely on authorization being performed at http handler level upon
    // receiving request.

    String userId = SecurityRequestContext.getUserId();
    LOG.info("Inside getPrincipal of SystemAuthenticationContext:userId: {}", userId);
    Credential userCredential = SecurityRequestContext.getUserCredential();
    LOG.info("Inside getPrincipal of SystemAuthenticationContext:userCredential: {}", userCredential);
    if (userId != null && userCredential != null) {
      return new Principal(userId, Principal.PrincipalType.USER, userCredential);
    } else if (userId != null && userCredential == null) {
      LOG.warn("Unexpected SecurityRequestContext state, userId = {} while userCredential = NULL",
          userId);
    } else if (userId == null && userCredential != null) {
      LOG.warn("Unexpected SecurityRequestContext state, userId = NULL while userCredential = {}",
          userCredential);
    }

    try {
      userId = UserGroupInformation.getCurrentUser().getShortUserName();
      LOG.info("serGroupInformation.getCurrentUser().getShortUserName(); userId: {}", userId);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    long currentTimestamp = System.currentTimeMillis();
    UserIdentity identity = new UserIdentity(userId, UserIdentity.IdentifierType.INTERNAL,
        Collections.emptyList(), currentTimestamp,
        currentTimestamp + DEFAULT_EXPIRATION);
    LOG.info("UserIdentity identity: {}", identity);
    AccessToken accessToken = tokenManager.signIdentifier(identity);
    LOG.info("AccessToken accessToken: {}", accessToken);
    String encodedAccessToken;
    try {
      encodedAccessToken = Base64.getEncoder().encodeToString(accessTokenCodec.encode(accessToken));
      Credential credential = new Credential(encodedAccessToken,
          Credential.CredentialType.INTERNAL);
      LOG.info("Credential credential: {}", credential);
      return new Principal(userId, Principal.PrincipalType.USER, credential);
    } catch (IOException e) {
      throw new RuntimeException("Unexpected failure while creating internal system identity", e);
    }
  }
}
