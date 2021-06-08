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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

public class AESCipherTest {
  protected static final Logger LOG = LoggerFactory.getLogger(TestTokenManager.class);

  @Test
  public void testEncryptionAndDecryption() throws CipherException {
    AESCipher cipher = new AESCipher();

    String password = "password for encryption";
    byte[] plainData = generateRandomBytes(2 * 1024);
    byte[] cipherData = cipher.encrypt(password, plainData);
    byte[] decryptedData = cipher.decrypt(password, cipherData);
    Assert.assertTrue(Arrays.equals(plainData, decryptedData));

    String plainString = new String(generateRandomBytes(64), StandardCharsets.UTF_8);
    cipherData = cipher.encrypt(password, plainString.getBytes());
    String decryptedString = new String(cipher.decrypt(password, cipherData), StandardCharsets.UTF_8);
    Assert.assertTrue(plainString.equals(decryptedString));
  }

  @Test(expected = CipherException.class)
  public void testDecryptionException() throws CipherException {
    AESCipher cipher = new AESCipher();

    String password = "password for encryption";
    byte[] plainData = generateRandomBytes(32);
    byte[] cipherData = cipher.encrypt(password, plainData);
    // Intentionally corrupt the encrypted data to cause exception.
    cipherData[0] = 0;
    byte[] decryptedData = cipher.decrypt(password, cipherData);
  }

  private byte[] generateRandomBytes(int len) {
    byte[] bytes = new byte[len];
    new Random().nextBytes(bytes);
    return bytes;
  }
}
