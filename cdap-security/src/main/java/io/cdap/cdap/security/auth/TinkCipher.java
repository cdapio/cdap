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

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import javax.inject.Inject;

/**
 * Tink cipher that allows encrypting and decrypting data using keyset stored in {@link SConfiguration}
 */
public class TinkCipher implements Cipher {
  /**
   * Wraps a keyset with some additional parameters and metadata.
   */
  private final KeysetHandle keysetHandle;

  /**
   * Tink AEAD (Authenticated Encryption with Associated Data) primitive
   */
  private final Aead aead;

  @Inject
  public TinkCipher(SConfiguration sConf) {
    try {
      // Init Tink with AEAD primitive
      AeadConfig.register();

      // Load keyset from sConf
      String jsonKeyset = sConf.get(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_KEYSET);
      this.keysetHandle = CleartextKeysetHandle.read(JsonKeysetReader.withString(jsonKeyset));

      this.aead = keysetHandle.getPrimitive(Aead.class);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create TinkCipher: " + e.getMessage(), e);
    }
  }

  /**
   * Encrypt the data.
   *
   * @param plainData data to be encrypted
   * @return encrypted data
   * @throws CipherException if encryption fails
   */
  public byte[] encrypt(byte[] plainData) throws CipherException {
    try {
      return aead.encrypt(plainData, null);
    } catch (GeneralSecurityException e) {
      throw new CipherException("Failed to encrypt: " + e.getMessage(), e);
    }
  }

  /**
   * Encrypt the string and return encrypted data in base64 encoded form.
   *
   * @param plainData data to be encrypted
   * @return encrypted data in base64 encoded form
   * @throws CipherException if encryption fails
   */
  public String encryptStringToBase64(String plainData) throws CipherException {
    return Base64.getEncoder().encodeToString(encrypt(plainData.getBytes()));
  }

  /**
   * Encrypt the data and return encrypted data in base64 encoded form.
   *
   * @param plainData data to be encrypted
   * @return encrypted data in base64 encoded form
   * @throws CipherException if encryption fails
   */
  public String encryptToBase64(byte[] plainData) throws CipherException {
    return Base64.getEncoder().encodeToString(encrypt(plainData));
  }

  /**
   * Decrypt the cipher data.
   *
   * @param cipherData data to be decrypted
   * @return decrypted data
   * @throws CipherException if decryption fails
   */
  public byte[] decrypt(byte[] cipherData) throws CipherException {
    try {
      return aead.decrypt(cipherData, null);
    } catch (GeneralSecurityException e) {
      throw new CipherException("Failed to decrypt: " + e.getMessage(), e);
    }
  }

  /**
   * Decrypt the cipher data that was base64 encoded.
   *
   * @param cipherData data in base64 encoded form that needs to be decrypted
   * @return decrypted data
   * @throws CipherException if decryption fails
   */
  public byte[] decryptFromBase64(String cipherData) throws CipherException {
    return decrypt(Base64.getDecoder().decode(cipherData));
  }

  /**
   * Decrypt the cipher data that was encrypted and base-encoded from a string.
   *
   * @param cipherData data in base64 encoded form that needs to be decrypted
   * @return decrypted data
   * @throws CipherException if decryption fails
   */
  public String decryptStringFromBase64(String cipherData) throws CipherException {
    return new String(decrypt(Base64.getDecoder().decode(cipherData)), StandardCharsets.UTF_8);
  }
}


