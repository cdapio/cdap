package com.continuuity.security.auth;

/**
 * Represents a message signed by a secret key.
 * @param <T> the type of the message object which has been signed.
 */
public interface Signed<T> {
  /**
   * Returns the message object which was signed.
   */
  T getMessage();

  /**
   * Returns the identifier for the secret key used to compute the message digest.
   */
  int getKeyId();

  /**
   * Returns the digest generated against the message.
   */
  byte[] getDigestBytes();
}
