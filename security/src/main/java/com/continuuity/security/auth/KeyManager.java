package com.continuuity.security.auth;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * Maintains secret keys used to sign and validate authentication tokens.
 */
public interface KeyManager {

  /**
   * Represents the combination of a digest computed on a message using a secret key, and the ID of the secret key
   * used to compute the digest.  Both elements are needed in order to later recompute (validate) the digest.
   */
  public static class DigestId {
    private final int id;
    private final byte[] digest;

    public DigestId(int id, byte[] digest) {
      this.id = id;
      this.digest = digest;
    }

    public int getId() {
      return id;
    }

    public byte[] getDigest() {
      return digest;
    }
  }

  /**
   * Instantiate the necessary instance variables.
   * @throws NoSuchAlgorithmException
   * @throws IOException
   */
  void init() throws NoSuchAlgorithmException, IOException;

  /**
   * Computes a digest for the given input message, using the current secret key.
   * @param message The data over which we should generate a digest.
   * @return The computed digest and the ID of the secret key used in generation.
   * @throws java.security.InvalidKeyException If the internal {@code Mac} implementation does not accept the given key.
   */
  DigestId generateMAC(byte[] message) throws InvalidKeyException;

  /**
   * Recomputes the digest for the given message and verifies that it matches the provided value.
   * @param codec The serialization utility to use in serializing the message when recomputing the digest
   * @param signedMessage The message and digest to validate.
   */
  <T> void validateMAC(Codec<T> codec, Signed<T> signedMessage)
    throws InvalidDigestException, InvalidKeyException;

}
