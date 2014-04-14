package com.continuuity.security.auth;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 *
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

  void init() throws NoSuchAlgorithmException, IOException;

  DigestId generateMAC(byte[] message) throws InvalidKeyException;

  <T> void validateMAC(Codec<T> codec, Signed<T> signedMessage)
    throws InvalidDigestException, InvalidKeyException;

}
