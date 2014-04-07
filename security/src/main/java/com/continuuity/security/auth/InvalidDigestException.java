package com.continuuity.security.auth;

/**
 * Exception thrown if an asserted message digest does not match the recomputed value, using the same secret key.
 * This can occur if a message digest has been forged (without knowledge of the correct secret key), or if the
 * message contents have been tampered with.
 */
public class InvalidDigestException extends Exception {
  public InvalidDigestException(String message) {
    super(message);
  }
}
