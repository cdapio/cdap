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

    String secret = "password for encryption";
    byte[] plainData = generateRandomBytes(2 * 1024);
    byte[] cipherData = cipher.encrypt(secret, plainData);
    byte[] decryptedData = cipher.decrypt(secret, cipherData);
    Assert.assertTrue(Arrays.equals(plainData, decryptedData));

    String plainString = new String(generateRandomBytes(64), StandardCharsets.UTF_8);
    cipherData = cipher.encrypt(secret, plainString.getBytes());
    String decryptedString = new String(cipher.decrypt(secret, cipherData), StandardCharsets.UTF_8);
    Assert.assertTrue(plainString.equals(decryptedString));
  }

  private byte[] generateRandomBytes(int len) {
    byte[] bytes = new byte[len];
    new Random().nextBytes(bytes);
    return bytes;
  }
}
