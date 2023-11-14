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
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.aead.AeadConfig;
import io.cdap.cdap.security.spi.encryption.AeadCipherContext;
import io.cdap.cdap.security.spi.encryption.CipherInitializationException;
import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Tink cipher that allows encrypting and decrypting data using keyset provided via properties.
 */
public class CleartextTinkCipherCryptor extends AbstractTinkAeadCipherCryptor {

  /**
   * References a keyset stored in secure properties.
   */
  public static final String TINK_CLEARTEXT_KEYSET_KEY = "tink.cleartext.keyset";

  @Override
  public String getName() {
    return "tink-cleartext";
  }

  @Override
  public Aead initializeAead(AeadCipherContext context) throws CipherInitializationException {
    try {
      // Init Tink with AEAD primitive
      AeadConfig.register();

      // Load keyset from secure properties
      String keysetJson = context.getSecureProperties().get(TINK_CLEARTEXT_KEYSET_KEY);
      if (keysetJson == null || keysetJson.isEmpty()) {
        throw new IllegalArgumentException(String.format("Tink keyset cannot be null or empty"));
      }
      KeysetHandle keysetHandle = CleartextKeysetHandle
          .read(JsonKeysetReader.withString(keysetJson));

      return keysetHandle.getPrimitive(Aead.class);
    } catch (GeneralSecurityException e) {
      throw new CipherInitializationException("Failed to init Tink cipher: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new CipherInitializationException("Failed to load Tink keyset: " + e.getMessage(), e);
    }
  }
}


