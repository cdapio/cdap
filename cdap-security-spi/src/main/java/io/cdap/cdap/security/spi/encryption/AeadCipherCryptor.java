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

package io.cdap.cdap.security.spi.encryption;

/**
 * An AEAD symmetric encryption primitive.
 */
public interface AeadCipherCryptor {

  /**
   * @return The name of the AeadCipherCryptor.
   */
  String getName();

  /**
   * Initialize the cipher.
   *
   * @throws CipherInitializationException If initialization fails.
   */
  void initialize(AeadCipherContext context) throws CipherInitializationException;

  /**
   * Encrypt the data.
   *
   * @param plainData      Data to be encrypted.
   * @param associatedData Used for integrity checking during decryption.
   * @return Encrypted data.
   * @throws CipherOperationException If encryption fails.
   */
  byte[] encrypt(byte[] plainData, byte[] associatedData) throws CipherOperationException;

  /**
   * Decrypt the data.
   *
   * @param cipherData     Data to be decrypted
   * @param associatedData Used for integrity checking, must be the same as that used during
   *                       encryption.
   * @return Decrypted data.
   * @throws CipherOperationException If decryption fails.
   */
  byte[] decrypt(byte[] cipherData, byte[] associatedData) throws CipherOperationException;
}
