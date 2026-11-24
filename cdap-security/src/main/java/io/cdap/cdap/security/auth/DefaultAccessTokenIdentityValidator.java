/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.security.auth;

import com.google.inject.Inject;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.security.spi.authentication.AccessTokenIdentityValidator;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authentication.ValidatedTokenIdentity;
import java.io.IOException;
import java.util.Base64;

/**
 * Default {@link AccessTokenIdentityValidator} backed by {@link TokenManager} and {@link
 * Codec} for {@link AccessToken}.
 */
public final class DefaultAccessTokenIdentityValidator implements AccessTokenIdentityValidator {

  private final TokenManager tokenManager;
  private final Codec<AccessToken> accessTokenCodec;

  /**
   * Creates a validator backed by the cluster {@link TokenManager} and {@link AccessToken} codec.
   *
   * @param tokenManager token manager used to verify token signatures and expiration
   * @param accessTokenCodec codec used to deserialize encoded access tokens
   */
  @Inject
  public DefaultAccessTokenIdentityValidator(TokenManager tokenManager,
      Codec<AccessToken> accessTokenCodec) {
    this.tokenManager = tokenManager;
    this.accessTokenCodec = accessTokenCodec;
  }

  @Override
  public ValidatedTokenIdentity validate(String encodedToken) throws UnauthenticatedException {
    if (encodedToken == null || encodedToken.isEmpty()) {
      throw new UnauthenticatedException("Access token is missing");
    }
    byte[] decodedBytes;
    try {
      decodedBytes = Base64.getDecoder().decode(encodedToken);
    } catch (IllegalArgumentException e) {
      throw new UnauthenticatedException("Malformed Base64 access token", e);
    }

    AccessToken accessToken;
    try {
      accessToken = accessTokenCodec.decode(decodedBytes);
    } catch (IOException | RuntimeException e) {
      throw new UnauthenticatedException("Failed to deserialize access token", e);
    }

    if (accessToken == null || accessToken.getIdentifier() == null) {
      throw new UnauthenticatedException("Decoded access token is missing user identity");
    }

    try {
      tokenManager.validateSecret(accessToken);
    } catch (InvalidTokenException e) {
      throw new UnauthenticatedException("Failed to validate access token: " + e.getMessage(), e);
    }

    UserIdentity identity = accessToken.getIdentifier();
    if (identity.getUsername() == null) {
      throw new UnauthenticatedException("Access token user identity has null username");
    }
    boolean internal = identity.getIdentifierType() == UserIdentity.IdentifierType.INTERNAL;
    return new ValidatedTokenIdentity(identity.getUsername(), internal);
  }
}
