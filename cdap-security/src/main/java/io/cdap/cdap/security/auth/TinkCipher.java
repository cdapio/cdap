/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.security.auth;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.aead.AeadConfig;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import javax.annotation.Nullable;

/**
 * Tink cipher that allows encrypting and decrypting data using keyset stored in {@link
 * SConfiguration}
 */
public class TinkCipher {

  /**
   * Wraps a keyset with some additional parameters and metadata.
   */
  private final KeysetHandle keysetHandle;

  /**
   * Tink AEAD (Authenticated Encryption with Associated Data) primitive
   */
  private final Aead aead;

  /**
   * Default associated data to use if none is specified
   */
  private static final byte[] DEFAULT_ASSOCIATED_DATA = "DefaultAssociatedData".getBytes(
      StandardCharsets.UTF_8);

  public TinkCipher(SConfiguration sConf) throws CipherException {
    try {
      // Init Tink with AEAD primitive
      AeadConfig.register();

      // Load keyset from sConf
      String jsonKeyset = sConf.get(
          Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_KEYSET);
      this.keysetHandle = CleartextKeysetHandle.read(JsonKeysetReader.withString(jsonKeyset));

      this.aead = keysetHandle.getPrimitive(Aead.class);
    } catch (GeneralSecurityException e) {
      throw new CipherException("Failed to init Tink cipher: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new CipherException("Failed to load Tink keyset: " + e.getMessage(), e);
    }
  }

  /**
   * Encrypt the data.
   *
   * @param plainData data to be encrypted
   * @param associatedData used for integrity checking during decryption.
   * @return encrypted data
   * @throws CipherException if encryption fails
   */
  public byte[] encrypt(byte[] plainData, @Nullable byte[] associatedData) throws CipherException {
    try {
      if (associatedData == null) {
        associatedData = DEFAULT_ASSOCIATED_DATA;
      }
      return aead.encrypt(plainData, associatedData);
    } catch (GeneralSecurityException e) {
      throw new CipherException("Failed to encrypt: " + e.getMessage(), e);
    }
  }

  /**
   * Encrypt the string and return encrypted data in base64 encoded form.
   *
   * @param plainData data to be encrypted
   * @param associatedData used for integrity checking during decryption.
   * @return encrypted data in base64 encoded form
   * @throws CipherException if encryption fails
   */
  public String encryptStringToBase64(String plainData, @Nullable byte[] associatedData)
      throws CipherException {
    return Base64.getEncoder().encodeToString(encrypt(plainData.getBytes(), associatedData));
  }

  /**
   * Encrypt the data and return encrypted data in base64 encoded form.
   *
   * @param plainData data to be encrypted
   * @param associatedData used for integrity checking during decryption.
   * @return encrypted data in base64 encoded form
   * @throws CipherException if encryption fails
   */
  public String encryptToBase64(byte[] plainData, @Nullable byte[] associatedData)
      throws CipherException {
    return Base64.getEncoder().encodeToString(encrypt(plainData, associatedData));
  }

  /**
   * Decrypt the cipher data.
   *
   * @param cipherData data to be decrypted
   * @param associatedData used for integrity checking, must be the same as that used during
   *     encryption.
   * @return decrypted data
   * @throws CipherException if decryption fails
   */
  public byte[] decrypt(byte[] cipherData, @Nullable byte[] associatedData) throws CipherException {
    try {
      if (associatedData == null) {
        associatedData = DEFAULT_ASSOCIATED_DATA;
      }
      return aead.decrypt(cipherData, associatedData);
    } catch (GeneralSecurityException e) {
      throw new CipherException("Failed to decrypt: " + e.getMessage(), e);
    }
  }

  /**
   * Decrypt the cipher data that was base64 encoded.
   *
   * @param cipherData data in base64 encoded form that needs to be decrypted
   * @param associatedData used for integrity checking, must be the same as that used during
   *     encryption.
   * @return decrypted data
   * @throws CipherException if decryption fails
   */
  public byte[] decryptFromBase64(String cipherData, byte[] associatedData) throws CipherException {
    return decrypt(Base64.getDecoder().decode(cipherData), associatedData);
  }

  /**
   * Decrypt the cipher data that was encrypted and base-encoded from a string.
   *
   * @param cipherData data in base64 encoded form that needs to be decrypted
   * @param associatedData used for integrity checking, must be the same as that used during
   *     encryption.
   * @return decrypted data
   * @throws CipherException if decryption fails
   */
  public String decryptStringFromBase64(String cipherData, byte[] associatedData)
      throws CipherException {
    return new String(decrypt(Base64.getDecoder().decode(cipherData), associatedData),
        StandardCharsets.UTF_8);
  }
}


