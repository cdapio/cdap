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
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class TinkCipherTest {
  protected static final Logger LOG = LoggerFactory.getLogger(TestTokenManager.class);

  @Test
  public void testSimpleEncryptionAndDecryption() throws CipherException, IOException, GeneralSecurityException {
    int totalIterations = 10;
    testEncryptionAndDecryption(totalIterations);
  }

  @Test(expected = IllegalStateException.class)
  public void testInitException() throws CipherException {
    SConfiguration sConf = SConfiguration.create();
    sConf.set(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_KEYSET, "invalid keyset");
    TinkCipher cipher = new TinkCipher(sConf);
  }

  @Test
  public void testDecryptExceptionCorruptionWithConcurrency() throws InterruptedException, ExecutionException {
    int numberOfThreads = 10;
    int iterationsPerThread = 100;
    ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
    CountDownLatch latch = new CountDownLatch(numberOfThreads);
    AtomicInteger exceptionCount = new AtomicInteger(0);
    for (int i = 0; i < numberOfThreads; i++) {
      executorService.submit(() -> {
        try {
          testEncryptionAndDecryption(iterationsPerThread);
        } catch (Exception e) {
          exceptionCount.getAndAdd(1);
        } finally {
          latch.countDown();
        }
      });
    }
    latch.await();
    Assert.assertEquals(0, exceptionCount.get());
  }

  private void testEncryptionAndDecryption(int totalIterations)
    throws CipherException, IOException, GeneralSecurityException {
    SConfiguration sConf = SConfiguration.create();
    sConf.set(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_KEYSET, generateKeySet());

    for (int iterCount = 0; iterCount < totalIterations; iterCount++) {
      TinkCipher cipher = new TinkCipher(sConf);

      byte[] plainData = generateRandomBytes(2 * 1024);
      byte[] cipherData = cipher.encrypt(plainData);
      byte[] decryptedData = cipher.decrypt(cipherData);
      Assert.assertArrayEquals(plainData, decryptedData);

      String cipherDataBase64Encoded = cipher.encryptToBase64(plainData);
      decryptedData = cipher.decryptFromBase64(cipherDataBase64Encoded);
      Assert.assertArrayEquals(plainData, decryptedData);
    }
  }

  @Test(expected = CipherException.class)
  public void testDecryptExceptionCorruption() throws CipherException, IOException, GeneralSecurityException {
    SConfiguration sConf = SConfiguration.create();
    sConf.set(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_KEYSET, generateKeySet());

    TinkCipher cipher = new TinkCipher(sConf);

    byte[] plainData = generateRandomBytes(128);
    byte[] associatedData = generateRandomBytes(64);
    byte[] cipherData = cipher.encrypt(plainData);
    // Intentionally corrupt the cipher data.
    cipherData[0] = 0;
    byte[] decryptedData = cipher.decrypt(cipherData);
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
