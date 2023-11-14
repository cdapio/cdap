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

package io.cdap.cdap.security.encryption.guice;

import io.cdap.cdap.security.encryption.AeadCipher;
import io.cdap.cdap.security.spi.encryption.AeadCipherContext;
import io.cdap.cdap.security.spi.encryption.AeadCipherCryptor;
import io.cdap.cdap.security.spi.encryption.CipherException;
import io.cdap.cdap.security.spi.encryption.CipherInitializationException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation of {@link AeadCipher} which delegates calls to an instance of the SPI
 * {@link AeadCipherCryptor} with lazy initialization.
 */
public class LazyDelegateAeadCipher implements AeadCipher {

  private final AeadCipherCryptor aeadCipherCryptor;
  private final Map<String, String> properties;
  private Map<String, String> secureProperties;
  private volatile boolean initialized;

  protected LazyDelegateAeadCipher(AeadCipherCryptor aeadCipherCryptor,
      Map<String, String> properties,
      Map<String, String> secureProperties) {
    this.aeadCipherCryptor = aeadCipherCryptor;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    this.secureProperties = Collections.unmodifiableMap(new HashMap<>(secureProperties));
  }

  private void lazyInitialize() throws CipherInitializationException {
    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          aeadCipherCryptor.initialize(new AeadCipherContext(properties, secureProperties));
          // Help garbage collect secure properties to avoid keeping them in memory.
          secureProperties = null;
          this.initialized = true;
        }
      }
    }
  }

  @Override
  public byte[] encrypt(byte[] plainData, byte[] associatedData) throws CipherException {
    lazyInitialize();
    return aeadCipherCryptor.encrypt(plainData, associatedData);
  }

  @Override
  public byte[] decrypt(byte[] cipherData, byte[] associatedData) throws CipherException {
    lazyInitialize();
    return aeadCipherCryptor.decrypt(cipherData, associatedData);
  }
}
