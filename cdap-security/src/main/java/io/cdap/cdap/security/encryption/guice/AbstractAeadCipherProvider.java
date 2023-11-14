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

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.security.encryption.AeadCipher;
import io.cdap.cdap.security.encryption.NoOpAeadCipher;
import java.util.Map;
import javax.inject.Provider;

/**
 * Provider for {@link AeadCipher}.
 */
public abstract class AbstractAeadCipherProvider implements Provider<AeadCipher> {

  private final String NOOP_AEAD_CIPHER_NAME = "NONE";

  private final AeadCipherCryptorExtensionLoader aeadCipherCryptorExtensionLoader;
  final CConfiguration cConf;
  final SConfiguration sConf;

  public AbstractAeadCipherProvider(
      AeadCipherCryptorExtensionLoader aeadCipherCryptorExtensionLoader,
      CConfiguration cConf, SConfiguration sConf) {
    this.cConf = cConf;
    this.sConf = sConf;
    this.aeadCipherCryptorExtensionLoader = aeadCipherCryptorExtensionLoader;
  }

  /**
   * Returns the AEAD cipher to use.
   *
   * @return The AEAD cipher to use.
   */
  protected abstract String getCipherName();

  /**
   * Returns the properties to pass to the cipher service.
   *
   * @return The properties to pass to the cipher service.
   */
  protected abstract Map<String, String> getProperties();

  /**
   * Returns the secure properties to pass to the cipher service.
   *
   * @return The secure properties to pass to the cipher service.
   */
  protected abstract Map<String, String> getSecureProperties();

  @Override
  public AeadCipher get() {
    String cipherName = getCipherName();
    if (NOOP_AEAD_CIPHER_NAME.equals(cipherName)) {
      return new NoOpAeadCipher();
    }
    return new LazyDelegateAeadCipher(aeadCipherCryptorExtensionLoader.get(cipherName),
        getProperties(), getSecureProperties());
  }
}
