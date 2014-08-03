/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.security.auth;

import co.cask.cdap.common.io.Codec;
import com.google.common.util.concurrent.Service;

import java.security.InvalidKeyException;

/**
 * Maintains secret keys used to sign and validate authentication tokens.
 */
public interface KeyManager extends Service {

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
