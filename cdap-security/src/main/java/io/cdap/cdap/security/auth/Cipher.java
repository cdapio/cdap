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

public interface Cipher {
  /**
   * Encrypt the string and return encrypted data in base64 encoded form.
   *
   * @param plainData data to be encrypted
   * @return encrypted data in base64 encoded form
   * @throws CipherException if encryption fails
   */
  String encryptStringToBase64(String plainData) throws CipherException;

  /**
   * Encrypt the data and return encrypted data in base64 encoded form.
   *
   * @param plainData data to be encrypted
   * @return encrypted data in base64 encoded form
   * @throws CipherException if encryption fails
   */
  String encryptToBase64(byte[] plainData) throws CipherException;

  /**
   * Decrypt the cipher data.
   *
   * @param cipherData data to be decrypted
   * @return decrypted data
   * @throws CipherException if decryption fails
   */
  byte[] decrypt(byte[] cipherData) throws CipherException;

  /**
   * Decrypt the cipher data that was base64 encoded.
   *
   * @param cipherData data in base64 encoded form that needs to be decrypted
   * @return decrypted data
   * @throws CipherException if decryption fails
   */
  byte[] decryptFromBase64(String cipherData) throws CipherException;

  /**
   * Decrypt the cipher data that was encrypted and base-encoded from a string.
   *
   * @param cipherData data in base64 encoded form that needs to be decrypted
   * @return decrypted data
   * @throws CipherException if decryption fails
   */
  String decryptStringFromBase64(String cipherData) throws CipherException;
}
