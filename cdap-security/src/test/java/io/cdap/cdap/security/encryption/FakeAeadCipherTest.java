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

import com.google.gson.Gson;
import io.cdap.cdap.security.spi.encryption.CipherOperationException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link FakeAeadCipher}.
 */
public class FakeAeadCipherTest {

  private static final Gson GSON = new Gson();
  private static FakeAeadCipher fakeAeadCipherService;

  @BeforeClass
  public static void setup() throws Exception {
    fakeAeadCipherService = new FakeAeadCipher();
    fakeAeadCipherService.initialize();
  }

  @Test
  public void testEncryptDecrypt() throws Exception {
    byte[] originalData = "some-data-is-here-20o397428976209873012973378965tousdhjbo9o"
        .getBytes(StandardCharsets.UTF_8);
    byte[] validAssociatedData = "valid-associated-data".getBytes(StandardCharsets.UTF_8);
    byte[] encryptedData = fakeAeadCipherService.encrypt(originalData, validAssociatedData);
    byte[] decryptedData = fakeAeadCipherService.decrypt(encryptedData, validAssociatedData);
    Assert.assertArrayEquals(originalData, decryptedData);
  }

  @Test(expected = CipherOperationException.class)
  public void testInvalidDecrypt() throws Exception {
    byte[] iv = new byte[16];
    byte[] ciphertext = "invalid-ciphertext".getBytes(StandardCharsets.UTF_8);
    SecureRandom secureRandom = new SecureRandom();
    secureRandom.nextBytes(iv);
    fakeAeadCipherService
        .decrypt(String.format("{\"iv\":%s,\"ciphertext\":%s}",
            GSON.toJson(iv), GSON.toJson(ciphertext)).getBytes(StandardCharsets.UTF_8),
            "invalid-associated-data".getBytes(StandardCharsets.UTF_8));
  }

  @Test(expected = CipherOperationException.class)
  public void testInvalidCiphertextFormat() throws Exception {
    fakeAeadCipherService.decrypt("invalid-ciphertext".getBytes(StandardCharsets.UTF_8),
        "invalid-associated-data".getBytes(StandardCharsets.UTF_8));
  }

  @Test(expected = CipherOperationException.class)
  public void testMismatchedAssociatedData() throws Exception {
    byte[] originalData = "2038979q8ahvwe09w37893o2uihieryhg890w8u09pfuijsdilhg8907u230w8u"
        .getBytes(StandardCharsets.UTF_8);
    byte[] validAssociatedData = "valid-associated-data".getBytes(StandardCharsets.UTF_8);
    byte[] encryptedData = fakeAeadCipherService.encrypt(originalData, validAssociatedData);
    fakeAeadCipherService
        .decrypt(encryptedData, "invalid-associated-data".getBytes(StandardCharsets.UTF_8));
  }
}
