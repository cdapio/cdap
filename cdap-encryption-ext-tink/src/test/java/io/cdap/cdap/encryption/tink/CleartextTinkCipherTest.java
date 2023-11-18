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

import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetWriter;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.aead.AeadConfig;
import io.cdap.cdap.security.spi.encryption.AeadCipherContext;
import io.cdap.cdap.security.spi.encryption.CipherInitializationException;
import io.cdap.cdap.security.spi.encryption.CipherOperationException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

public class CleartextTinkCipherTest {

  @Test
  public void testEncryptionAndDecryption() throws CipherInitializationException,
      CipherOperationException, IOException, GeneralSecurityException {
    Map<String, String> properties = new HashMap<>();
    Map<String, String> secureProperties = new HashMap<>();
    secureProperties.put(CleartextTinkCipherCryptor.TINK_CLEARTEXT_KEYSET_KEY, generateKeySet());

    CleartextTinkCipherCryptor cipher = new CleartextTinkCipherCryptor();
    cipher.initialize(new AeadCipherContext(properties, secureProperties));

    byte[] plainData = generateRandomBytes(2 * 1024);
    byte[] associatedData = generateRandomBytes(64);
    byte[] cipherData = cipher.encrypt(plainData, associatedData);
    byte[] decryptedData = cipher.decrypt(cipherData, associatedData);
    Assert.assertArrayEquals(plainData, decryptedData);
  }

  @Test(expected = CipherInitializationException.class)
  public void testInitException() throws CipherInitializationException {
    Map<String, String> properties = new HashMap<>();
    Map<String, String> secureProperties = new HashMap<>();
    secureProperties.put(CleartextTinkCipherCryptor.TINK_CLEARTEXT_KEYSET_KEY, "invalid-keyset");

    CleartextTinkCipherCryptor cipher = new CleartextTinkCipherCryptor();
    cipher.initialize(new AeadCipherContext(properties, secureProperties));
  }

  @Test(expected = CipherOperationException.class)
  public void testDecryptExceptionTagMismatch() throws CipherInitializationException,
      CipherOperationException, IOException, GeneralSecurityException {
    Map<String, String> properties = new HashMap<>();
    Map<String, String> secureProperties = new HashMap<>();
    secureProperties.put(CleartextTinkCipherCryptor.TINK_CLEARTEXT_KEYSET_KEY, generateKeySet());

    CleartextTinkCipherCryptor cipher = new CleartextTinkCipherCryptor();
    cipher.initialize(new AeadCipherContext(properties, secureProperties));

    byte[] plainData = generateRandomBytes(128);
    byte[] associatedData = generateRandomBytes(64);
    byte[] cipherData = cipher.encrypt(plainData, associatedData);
    byte[] invalidAssociatedData = generateRandomBytes(64);
    cipher.decrypt(cipherData, invalidAssociatedData);
  }

  @Test(expected = CipherOperationException.class)
  public void testDecryptExceptionCorruption() throws CipherInitializationException,
      CipherOperationException, IOException, GeneralSecurityException {
    Map<String, String> properties = new HashMap<>();
    Map<String, String> secureProperties = new HashMap<>();
    secureProperties.put(CleartextTinkCipherCryptor.TINK_CLEARTEXT_KEYSET_KEY, generateKeySet());

    CleartextTinkCipherCryptor cipher = new CleartextTinkCipherCryptor();
    cipher.initialize(new AeadCipherContext(properties, secureProperties));

    byte[] plainData = generateRandomBytes(128);
    byte[] associatedData = generateRandomBytes(64);
    byte[] cipherData = cipher.encrypt(plainData, associatedData);
    // Intentionally corrupt the cipher data.
    cipherData[0] = 0;
    byte[] decryptedData = cipher.decrypt(cipherData, associatedData);
  }

  private String generateKeySet() throws IOException, GeneralSecurityException {
    AeadConfig.register();
    KeysetHandle keysetHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"));
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    CleartextKeysetHandle.write(keysetHandle, JsonKeysetWriter.withOutputStream(outputStream));
    return outputStream.toString();
  }

  private byte[] generateRandomBytes(int len) {
    byte[] bytes = new byte[len];
    new Random().nextBytes(bytes);
    return bytes;
  }
}
