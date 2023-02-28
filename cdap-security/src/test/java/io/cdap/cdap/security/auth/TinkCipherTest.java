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

import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetWriter;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.aead.AeadConfig;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

public class TinkCipherTest {
  @Test
  public void testEncryptionAndDecryption() throws CipherException, IOException, GeneralSecurityException {
    SConfiguration sConf = SConfiguration.create();
    sConf.set(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_KEYSET, generateKeySet());

    TinkCipher cipher = new TinkCipher(sConf);

    byte[] plainData = generateRandomBytes(2 * 1024);
    byte[] associatedData = generateRandomBytes(64);
    byte[] cipherData = cipher.encrypt(plainData, associatedData);
    byte[] decryptedData = cipher.decrypt(cipherData, associatedData);
    Assert.assertArrayEquals(plainData, decryptedData);

    String cipherDataBase64Encoded = cipher.encryptToBase64(plainData, associatedData);
    decryptedData = cipher.decryptFromBase64(cipherDataBase64Encoded, associatedData);
    Assert.assertArrayEquals(plainData, decryptedData);
  }

  @Test(expected = CipherException.class)
  public void testInitException() throws CipherException {
    SConfiguration sConf = SConfiguration.create();
    sConf.set(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_KEYSET, "invalid keyset");

    TinkCipher cipher = new TinkCipher(sConf);
  }

  @Test(expected = CipherException.class)
  public void testDecryptExceptionTagMismatch() throws CipherException, IOException, GeneralSecurityException {
    SConfiguration sConf = SConfiguration.create();
    sConf.set(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_KEYSET, generateKeySet());

    TinkCipher cipher = new TinkCipher(sConf);

    byte[] plainData = generateRandomBytes(128);
    byte[] associatedData = generateRandomBytes(64);
    byte[] cipherData = cipher.encrypt(plainData, associatedData);
    byte[] invalidAssociatedData = generateRandomBytes(64);
    byte[] decryptedData = cipher.decrypt(cipherData, invalidAssociatedData);
  }

  @Test(expected = CipherException.class)
  public void testDecryptExceptionCorruption() throws CipherException, IOException, GeneralSecurityException {
    SConfiguration sConf = SConfiguration.create();
    sConf.set(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_KEYSET, generateKeySet());

    TinkCipher cipher = new TinkCipher(sConf);

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
