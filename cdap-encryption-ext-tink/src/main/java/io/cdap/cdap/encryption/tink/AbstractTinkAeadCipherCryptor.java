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

package io.cdap.cdap.encryption.tink;

import com.google.crypto.tink.Aead;
import io.cdap.cdap.security.spi.encryption.AeadCipherContext;
import io.cdap.cdap.security.spi.encryption.AeadCipherCryptor;
import io.cdap.cdap.security.spi.encryption.CipherInitializationException;
import io.cdap.cdap.security.spi.encryption.CipherOperationException;
import java.security.GeneralSecurityException;

/**
 * An {@link AeadCipherCryptor} backed by Tink.
 */
public abstract class AbstractTinkAeadCipherCryptor implements AeadCipherCryptor {

  /**
   * Tink AEAD (Authenticated Encryption with Associated Data) primitive.
   */
  private Aead aead;

  /**
   * Initializes and returns an {@link Aead} primitive.
   *
   * @return An AEAD primitive for encryption.
   */
  protected abstract Aead initializeAead(AeadCipherContext context)
      throws CipherInitializationException;

  @Override
  public void initialize(AeadCipherContext context) throws CipherInitializationException {
    this.aead = initializeAead(context);
  }

  /**
   * Encrypt the data.
   *
   * @param plainData      data to be encrypted
   * @param associatedData used for integrity checking during decryption.
   * @return encrypted data
   * @throws CipherOperationException if encryption fails
   */
  @Override
  public byte[] encrypt(byte[] plainData, byte[] associatedData)
      throws CipherOperationException {
    try {
      return aead.encrypt(plainData, associatedData);
    } catch (GeneralSecurityException e) {
      throw new CipherOperationException("Failed to encrypt: " + e.getMessage(), e);
    }
  }

  /**
   * Decrypt the cipher data.
   *
   * @param cipherData     data to be decrypted
   * @param associatedData used for integrity checking, must be the same as that used during
   *                       encryption.
   * @return decrypted data
   * @throws CipherOperationException if decryption fails
   */
  @Override
  public byte[] decrypt(byte[] cipherData, byte[] associatedData)
      throws CipherOperationException {
    try {
      return aead.decrypt(cipherData, associatedData);
    } catch (GeneralSecurityException e) {
      throw new CipherOperationException("Failed to decrypt: " + e.getMessage(), e);
    }
  }
}
