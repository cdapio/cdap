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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * AES cipher that supports encryption and decryption of data.
 */
public class AESCipher {
  private static final String AES = "AES";
  private static final String ENCRYPTION_ALGORITHM = "AES/CBC/PKCS5PADDING";

  public static final int IV_LENGTH_BYTES = 16;

  private static final String PBE_ALGORITHM = "PBKDF2WithHmacSHA256";
  private static final int PBE_ITERATION_COUNT = 10000;
  private static final int PBE_KEY_LENGTH = 256;
  private static final int PBE_SALT_LENGTH_BYTES = 32;

  private static final Charset UTF_8 = StandardCharsets.UTF_8;

  private final SecureRandom rand;

  public AESCipher() {
    rand = new SecureRandom();
  }

  /**
   * Encrypt the data in the form of a string using the provided secret (i.e. password)
   *
   * @param secret the secret used for encrypting the data
   * @param data the data to be encrypted
   * @return data in encrypted form
   * @throws CipherException if encryption fails
   */
  public String encrypt(String secret, String data) throws CipherException {
    return new String(encrypt(secret, data.getBytes(UTF_8)), UTF_8);
  }

  /**
   * Encrypt the data in the form of byte array using the provided secret (i.e. password)
   *
   * @param secret the secret used for encrypting the data
   * @param data the data to be encrypted
   * @return data in encrypted form
   * @throws CipherException if encryption fails
   */
  public byte[] encrypt(String secret, byte[] data) throws CipherException {
    byte[] iv = new SecureRandom().generateSeed(IV_LENGTH_BYTES);
    byte[] salt = generateSalt(PBE_SALT_LENGTH_BYTES);
    byte[] cipherText = encrypt(secret, salt, iv, data);
    byte[] cipherTextWithPrefix =
      ByteBuffer.allocate(iv.length + salt.length + cipherText.length).put(iv).put(salt).put(cipherText).array();
    return cipherTextWithPrefix;
  }

  /**
   * Encrypt the data using the provided secret, salt, init vector.
   * salt is used along with secret to create the encryption key.
   */
  private byte[] encrypt(String secret, byte[] salt, byte[] initVector, byte[] data) throws CipherException {
    try {
      Cipher cipher = getCipher(secret, salt, initVector, Cipher.ENCRYPT_MODE);
      return cipher.doFinal(data);
    } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidAlgorithmParameterException
      | InvalidKeyException | IllegalBlockSizeException | BadPaddingException | InvalidKeySpecException e) {
      throw new CipherException("Failed to encrypt", e);
    }
  }

  /**
   * Encrypt the data in the form of a string using the provided secret (i.e. password)
   *
   * @param secret the secret that was used to encrypt the data
   * @param data the data to be decrypted
   * @return data in decrypted form
   * @throws CipherException if decryption fails
   */
  public String decrypt(String secret, String data) throws CipherException {
    return new String(decrypt(secret, data.getBytes(UTF_8)), UTF_8);
  }

  /**
   * Encrypt the data in the form of byte array using the provided secret (i.e. password)
   *
   * @param secret the secret that was used to encrypt the data
   * @param data the data to be decrypted
   * @return data in decrypted form
   * @throws CipherException if decryption fails
   */
  public byte[] decrypt(String secret, byte[] data) throws CipherException {
    ByteBuffer buf = ByteBuffer.wrap(data);

    byte[] iv = new byte[IV_LENGTH_BYTES];
    buf.get(iv);

    byte[] salt = new byte[PBE_SALT_LENGTH_BYTES];
    buf.get(salt);

    byte[] cipherText = new byte[buf.remaining()];
    buf.get(cipherText);

    byte[] plainText = decrypt(secret, salt, iv, cipherText);
    return plainText;
  }

  /**
   * Decrypt the data using the provided secret, salt, init vector.
   */
  private byte[] decrypt(String secret, byte[] salt, byte[] initVector, byte[] data)
    throws CipherException {
    try {
      Cipher cipher = this.getCipher(secret, salt, initVector, Cipher.DECRYPT_MODE);
      return cipher.doFinal(data);
    } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidAlgorithmParameterException
      | InvalidKeyException | BadPaddingException | IllegalBlockSizeException | InvalidKeySpecException e) {
      throw new CipherException("Failed to decrypt", e);
    }
  }

  /**
   * Build encryption key from the provided secret (i.e. password) and salt.
   */
  private SecretKey getKeyFromSecret(String secret, byte[] salt)
    throws NoSuchAlgorithmException, InvalidKeySpecException {
    SecretKeyFactory factory = SecretKeyFactory.getInstance(PBE_ALGORITHM);
    KeySpec spec = new PBEKeySpec(secret.toCharArray(), salt, PBE_ITERATION_COUNT, PBE_KEY_LENGTH);
    return new SecretKeySpec(factory.generateSecret(spec).getEncoded(), AES);
  }

  /**
   * Generate salt that used along with a shared secret to producer an encryption key.
   */
  private byte[] generateSalt(int length) {
    byte[] salt = new byte[length];
    rand.nextBytes(salt);
    return salt;
  }

  /**
   * Get a {@link Cipher} implementation for mode using the provided secret, salt, initVecotr.
   */
  private Cipher getCipher(String secret, byte[] salt, byte[] iv, int mode) throws InvalidAlgorithmParameterException,
    InvalidKeyException, NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeySpecException {
    IvParameterSpec ivSpec = new IvParameterSpec(iv);
    Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM);
    cipher.init(mode, getKeyFromSecret(secret, salt), ivSpec);
    return cipher;
  }
}
