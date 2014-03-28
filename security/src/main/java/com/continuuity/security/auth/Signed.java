package com.continuuity.security.auth;

/**
 * Represents a message signed by a secret key.
 */
public interface Signed {
  /**
   * Returns the raw message contents which were signed.
   */
  byte[] getMessageBytes();

  /**
   * Returns the identifier for the secret key used to compute the message digest.
   */
  int getKeyId();

  /**
   * Returns the digest generated against the message.
   */
  byte[] getDigestBytes();
}
