package com.continuuity.security.auth;

import com.continuuity.common.io.Codec;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;

import java.io.IOException;
import java.security.InvalidKeyException;

/**
 * Provides a simple interface to generate and validate {@link AccessToken}s.
 */
public class TokenManager extends AbstractIdleService {

  protected final KeyManager keyManager;
  private final Codec<AccessTokenIdentifier> identifierCodec;

  @Inject
  public TokenManager(KeyManager keyManager, Codec<AccessTokenIdentifier> identifierCodec) {
    this.keyManager = keyManager;
    this.identifierCodec = identifierCodec;
  }

  @Override
  public void startUp() {
    this.keyManager.startAndWait();
  }

  @Override
  public void shutDown() {
    this.keyManager.stopAndWait();
  }

  /**
   * Generates a signature for the given token value, using the currently active secret key.
   * @param identifier Verified identity for which a token should be generated.
   * @return A token containing the verified identify and a digest of its contents.
   */
  public AccessToken signIdentifier(AccessTokenIdentifier identifier) {
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
