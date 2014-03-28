package com.continuuity.security.auth;

import com.continuuity.api.common.Bytes;
import com.google.inject.Inject;

import java.security.InvalidKeyException;

/**
 * Provides a simple interface to generate and validate {@link AccessToken}s.
 */
public class TokenManager {

  private final KeyManager keyManager;

  @Inject
  public TokenManager(KeyManager keyManager) {
    this.keyManager = keyManager;
  }

  /**
   * Generates a signature for the given token value, using the currently active secret key.
   * @param identifier Verified identity for which a token should be generated.
   * @return A token containing the verified identify and a digest of its contents.
   */
  public AccessToken signIdentifier(AccessTokenIdentifier identifier) {
    try {
      KeyManager.DigestId digest = keyManager.generateMAC(identifier.toBytes());
      return new AccessToken(identifier, digest.getId(), digest.getDigest());
    } catch (InvalidKeyException ike) {
      throw new IllegalStateException("Invalid key configured for KeyManager", ike);
    }
  }

  /**
   * Given an {@link AccessToken} instance, checks that the token has not yet expired and that the digest matches
   * the expected value.  To validate the token digest, we recompute the digest value, based on the asserted identity
   * and our own view of the secret keys.
   * @param token The token instance to validate.
   * @throws InvalidTokenException If the provided token instance is expired or the digest does not match the
   * recomputed value.
   */
  public void validateSecret(AccessToken token) throws InvalidTokenException {
    long now = System.currentTimeMillis();
    if (token.getIdentifier().getExpireTimestamp() < now) {
      throw new InvalidTokenException(InvalidTokenException.Reason.EXPIRED, "Token is expired.");
    }

    try {
      keyManager.validateMAC(token);
    } catch (InvalidDigestException ide) {
      throw new InvalidTokenException(InvalidTokenException.Reason.INVALID, "Token signature is not valid!");
    } catch (InvalidKeyException ike) {
      throw new InvalidTokenException(InvalidTokenException.Reason.INTERNAL, "Invalid key for token.", ike);
    }
  }
}
