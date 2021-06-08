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
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * AES cipher that supports encryption and decryption of data.
 */
public class AESCipher {
  private static final String AES = "AES";
  private static final String ENCRYPTION_ALGORITHM = "AES/GCM/NoPadding";

  private static final int GCM_IV_LENGTH_BYTES = 12;
  private static final int GCM_TAG_LENGTH_BYTES = 16;

  private static final String PBE_ALGORITHM = "PBKDF2WithHmacSHA256";
  private static final int PBE_ITERATION_COUNT = 65536;
  private static final int PBE_KEY_LENGTH = 256;
  private static final int PBE_SALT_LENGTH_BYTES = 32;

  private final SecureRandom rand;

  public AESCipher() {
    rand = new SecureRandom();
  }

  /**
   * Encrypt the data using the provided password.
   *
   * @param password the password used for encrypting the data
   * @param plainData the data to be encrypted
   * @return data in encrypted form
   * @throws CipherException if encryption fails
   */
  public byte[] encrypt(String password, byte[] plainData) throws CipherException {
    byte[] iv = new SecureRandom().generateSeed(GCM_IV_LENGTH_BYTES);
    byte[] salt = generateSalt(PBE_SALT_LENGTH_BYTES);
    byte[] cipherData = encrypt(password, salt, iv, plainData);
    byte[] cipherDataWithPrefix =
      ByteBuffer.allocate(iv.length + salt.length + cipherData.length).put(iv).put(salt).put(cipherData).array();
    return cipherDataWithPrefix;
  }

  /**
   * Encrypt the data using the provided password, salt, init vector.
   * Salt is used along with password to create the encryption key.
   */
  private byte[] encrypt(String password, byte[] salt, byte[] initVector, byte[] plainData) throws CipherException {
    try {
      Cipher cipher = getCipher(password, salt, initVector, Cipher.ENCRYPT_MODE);
      return cipher.doFinal(plainData);
    } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidAlgorithmParameterException
      | InvalidKeyException | IllegalBlockSizeException | BadPaddingException | InvalidKeySpecException e) {
      throw new CipherException("Failed to encrypt", e);
    }
  }

  /**
   * Decrypt the data using the provided password.
   *
   * @param password the password used to encrypt the data
   * @param cipherData the data to be decrypted
   * @return data in decrypted form
   * @throws CipherException if decryption fails
   */
  public byte[] decrypt(String password, byte[] cipherData) throws CipherException {
    ByteBuffer buf = ByteBuffer.wrap(cipherData);

    byte[] iv = new byte[GCM_IV_LENGTH_BYTES];
    buf.get(iv);

    byte[] salt = new byte[PBE_SALT_LENGTH_BYTES];
    buf.get(salt);

    byte[] cipherText = new byte[buf.remaining()];
    buf.get(cipherText);

    byte[] plainText = decrypt(password, salt, iv, cipherText);
    return plainText;
  }

  /**
   * Decrypt the data using the provided password, salt, init vector.
   */
  private byte[] decrypt(String password, byte[] salt, byte[] initVector, byte[] data) throws CipherException {
    try {
      Cipher cipher = this.getCipher(password, salt, initVector, Cipher.DECRYPT_MODE);
      return cipher.doFinal(data);
    } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidAlgorithmParameterException
      | InvalidKeyException | BadPaddingException | IllegalBlockSizeException | InvalidKeySpecException e) {
      throw new CipherException("Failed to decrypt", e);
    }
  }

  /**
   * Build encryption key from the provided password and salt.
   */
  private SecretKeySpec getKeyFromSecret(String password, byte[] salt)
    throws NoSuchAlgorithmException, InvalidKeySpecException {
    SecretKeyFactory factory = SecretKeyFactory.getInstance(PBE_ALGORITHM);
    KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, PBE_ITERATION_COUNT, PBE_KEY_LENGTH);
    return new SecretKeySpec(factory.generateSecret(spec).getEncoded(), AES);
  }

  /**
   * Generate salt that is used along with a password to produce an encryption key.
   */
  private byte[] generateSalt(int length) {
    byte[] salt = new byte[length];
    rand.nextBytes(salt);
    return salt;
  }

  /**
   * Get a {@link Cipher} implementation for mode using the provided password, salt, initVector.
   */
  private Cipher getCipher(String password, byte[] salt, byte[] iv, int mode)
    throws InvalidAlgorithmParameterException, InvalidKeyException, NoSuchPaddingException,
    NoSuchAlgorithmException, InvalidKeySpecException {
    Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM);
    GCMParameterSpec paramSpec = new GCMParameterSpec(GCM_TAG_LENGTH_BYTES * 8, iv);
    cipher.init(mode, getKeyFromSecret(password, salt), paramSpec);
    return cipher;
  }
}
