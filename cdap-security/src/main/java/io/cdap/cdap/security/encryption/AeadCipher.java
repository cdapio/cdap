/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.security.encryption;

import io.cdap.cdap.security.spi.encryption.CipherException;
import io.cdap.cdap.security.spi.encryption.CipherOperationException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * An interface intended to represent a service providing AEAD symmetric encryption primitives.
 */
public interface AeadCipher {

  /**
   * Encrypt the data.
   *
   * @param plainData      Data to be encrypted.
   * @param associatedData Used for integrity checking during decryption.
   * @return Encrypted data.
   * @throws CipherOperationException If encryption fails.
   */
  byte[] encrypt(byte[] plainData, byte[] associatedData) throws CipherException;

  /**
   * Decrypt the data.
   *
   * @param cipherData     Data to be decrypted
   * @param associatedData Used for integrity checking, must be the same as that used during
   *                       encryption.
   * @return Decrypted data.
   * @throws CipherOperationException If decryption fails.
   */
  byte[] decrypt(byte[] cipherData, byte[] associatedData) throws CipherException;

  /**
   * Encrypt the string and return encrypted data in base64 encoded form.
   *
   * @param plainData      data to be encrypted
   * @param associatedData used for integrity checking during decryption.
   * @return encrypted data in base64 encoded form
   * @throws CipherOperationException if encryption fails
   */
  default String encryptToBase64(String plainData, byte[] associatedData)
      throws CipherException {
    return Base64.getEncoder().encodeToString(encrypt(plainData.getBytes(StandardCharsets.UTF_8),
        associatedData));
  }

  /**
   * Encrypt the data and return encrypted data in base64 encoded form.
   *
   * @param plainData      data to be encrypted
   * @param associatedData used for integrity checking during decryption.
   * @return encrypted data in base64 encoded form
   * @throws CipherOperationException if encryption fails
   */
  default String encryptToBase64(byte[] plainData, byte[] associatedData) throws CipherException {
    return Base64.getEncoder().encodeToString(encrypt(plainData, associatedData));
  }

  /**
   * Decrypt the cipher data that was base64 encoded.
   *
   * @param cipherData     data in base64 encoded form that needs to be decrypted
   * @param associatedData used for integrity checking, must be the same as that used during
   *                       encryption.
   * @return decrypted data
   * @throws CipherOperationException if decryption fails
   */
  default byte[] decryptFromBase64(String cipherData, byte[] associatedData)
      throws CipherException {
    return decrypt(Base64.getDecoder().decode(cipherData), associatedData);
  }

  /**
   * Decrypt the cipher data that was encrypted and base-encoded from a string.
   *
   * @param cipherData     data in base64 encoded form that needs to be decrypted
   * @param associatedData used for integrity checking, must be the same as that used during
   *                       encryption.
   * @return decrypted data
   * @throws CipherOperationException if decryption fails
   */
  default String decryptStringFromBase64(String cipherData, byte[] associatedData)
      throws CipherException {
    return new String(decrypt(Base64.getDecoder().decode(cipherData), associatedData),
        StandardCharsets.UTF_8);
  }
}
