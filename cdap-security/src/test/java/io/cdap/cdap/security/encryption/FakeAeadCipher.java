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
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.security.spi.encryption.CipherOperationException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

/**
 * An {@link AeadCipher} used for testing. NOTE: DO NOT USE THIS OUTSIDE OF UNIT TESTS; THIS IS NOT
 * INTENDED TO BE SECURE.
 */
public class FakeAeadCipher implements AeadCipher {

  private static final Gson GSON = new Gson();
  // Prefix for validating that the decryption took place successfully.
  // This is necessary for throwing an exception.
  private static final String VALIDATION_PREFIX = "validation-prefix";

  private SecureRandom secureRandom;
  private SecretKey secretKey;

  public FakeAeadCipher() {
    secureRandom = new SecureRandom();
  }

  public void initialize() throws NoSuchAlgorithmException {
    KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
    keyGenerator.init(256);
    secretKey = keyGenerator.generateKey();
  }

  @Override
  public byte[] encrypt(byte[] plainData, byte[] associatedData) throws CipherOperationException {
    Cipher cipher = createCipher();
    byte[] iv = generateIv();
    try {
      cipher.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(iv));
    } catch (Exception e) {
      throw new CipherOperationException("Failed to initialize Cipher", e);
    }
    byte[] encrypted;
    try {
      encrypted = cipher.doFinal(encodePlaintext(plainData, associatedData));
    } catch (Exception e) {
      throw new CipherOperationException("Failed to encrypt data", e);
    }
    return encodeCiphertext(iv, encrypted);
  }

  @Override
  public byte[] decrypt(byte[] cipherData, byte[] associatedData) throws CipherOperationException {
    Cipher cipher = createCipher();
    CipherTextWrapper cipherText = decodeCiphertext(cipherData);
    try {
      cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(cipherText.iv));
    } catch (Exception e) {
      throw new CipherOperationException("Failed to initialize Cipher", e);
    }

    byte[] decrypted;
    try {
      decrypted = cipher.doFinal(cipherText.ciphertext);
    } catch (Exception e) {
      throw new CipherOperationException("Failed to decrypt data", e);
    }

    PlainTextWrapper plainTextWrapper = decodePlaintext(decrypted);
    if (!Arrays.equals(associatedData, plainTextWrapper.associatedData)) {
      throw new CipherOperationException(
          String.format(
              "Failed to validate decrypted ciphertext: unexpected associated data: want '%s', "
                  + "got '%s'",
              new String(associatedData, StandardCharsets.UTF_8),
              new String(plainTextWrapper.associatedData, StandardCharsets.UTF_8)));
    }
    return plainTextWrapper.plainData;
  }

  private byte[] generateIv() {
    byte[] iv = new byte[16];
    secureRandom.nextBytes(iv);
    return iv;
  }

  private Cipher createCipher() throws CipherOperationException {
    Cipher cipher;
    try {
      cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
    } catch (Exception e) {
      throw new CipherOperationException("Failed to create Cipher", e);
    }
    return cipher;
  }

  private class PlainTextWrapper {

    private final byte[] plainData;
    private final byte[] associatedData;

    private PlainTextWrapper(byte[] plainData, byte[] associatedData) {
      this.plainData = plainData;
      this.associatedData = associatedData;
    }
  }

  private byte[] encodePlaintext(byte[] plainData, byte[] associatedData) {
    return (VALIDATION_PREFIX + GSON.toJson(new PlainTextWrapper(plainData, associatedData)))
        .getBytes(StandardCharsets.UTF_8);
  }

  private PlainTextWrapper decodePlaintext(byte[] plaintext) throws CipherOperationException {
    String plaintextStr = new String(plaintext, StandardCharsets.UTF_8);
    if (!plaintextStr.startsWith(VALIDATION_PREFIX)) {
      throw new CipherOperationException("Decryption failed! Plaintext does not start with "
          + "expected prefix.");
    }
    return GSON.fromJson(plaintextStr.substring(VALIDATION_PREFIX.length()),
        PlainTextWrapper.class);
  }

  private class CipherTextWrapper {

    private final byte[] iv;
    private final byte[] ciphertext;

    public CipherTextWrapper(byte[] iv, byte[] ciphertext) {
      this.iv = iv;
      this.ciphertext = ciphertext;
    }
  }

  private byte[] encodeCiphertext(byte[] iv, byte[] ciphertext) {
    return GSON.toJson(new CipherTextWrapper(iv, ciphertext))
        .getBytes(StandardCharsets.UTF_8);
  }

  private CipherTextWrapper decodeCiphertext(byte[] ciphertext) throws CipherOperationException {
    try {
      return GSON.fromJson(new String(ciphertext, StandardCharsets.UTF_8), CipherTextWrapper.class);
    } catch (JsonSyntaxException e) {
      throw new CipherOperationException("Decryption failed! Ciphertext not in JSON format.", e);
    }
  }
}
