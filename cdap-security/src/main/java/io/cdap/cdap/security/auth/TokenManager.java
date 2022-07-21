/*
 * Copyright © 2014-2021 Cask Data, Inc.
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

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.common.io.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidKeyException;

/**
 * Provides a simple interface to generate and validate {@link AccessToken}s.
 */
public class TokenManager extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(TokenManager.class);

  protected final KeyManager keyManager;
  private final Codec<UserIdentity> identifierCodec;

  @Inject
  public TokenManager(KeyManager keyManager, Codec<UserIdentity> identifierCodec) {
    this.keyManager = keyManager;
    this.identifierCodec = identifierCodec;
  }

  @Override
  public void startUp() {
    LOG.info("Starting TokenManager service");
    this.keyManager.startAndWait();
  }

  @Override
  public void shutDown() {
    LOG.info("Shutting down TokenManager service.");
    this.keyManager.stopAndWait();
  }

  /**
   * Generates a signature for the given token value, using the currently active secret key.
   * @param identifier Verified identity for which a token should be generated.
   * @return A token containing the verified identify and a digest of its contents.
   */
  public AccessToken signIdentifier(UserIdentity identifier) {
    try {
      KeyManager.DigestId digest = keyManager.generateMAC(identifierCodec.encode(identifier));
      return new AccessToken(identifier, digest.getId(), digest.getDigest());
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    } catch (InvalidKeyException ike) {
      throw new IllegalStateException("Invalid key configured for KeyManager.", ike);
    }
  }

  /**
   * Given an {@link AccessToken} instance, checks that the token has not yet expired and that the digest matches
   * the expected value. To validate the token digest, we recompute the digest value, based on the asserted identity
   * and our own view of the secret keys.
   * @param token The token instance to validate.
   * @throws InvalidTokenException If the provided token instance is expired or the digest does not match the
   * recomputed value.
   */
  public void validateSecret(AccessToken token) throws InvalidTokenException {
    long now = System.currentTimeMillis();
    if (token.getIdentifier().getExpireTimestamp() < now) {
      throw new InvalidTokenException(TokenState.EXPIRED, "Token is expired.");
    }

    try {
      keyManager.validateMAC(identifierCodec, token);
    } catch (InvalidDigestException ide) {
      throw new InvalidTokenException(TokenState.INVALID, "Token signature is not valid!");
    } catch (InvalidKeyException ike) {
      throw new InvalidTokenException(TokenState.INTERNAL, "Invalid key for token.", ike);
    }
  }
}
