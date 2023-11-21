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
import io.cdap.cdap.common.conf.Constants.Security.Encryption;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.security.encryption.AeadCipher;
import java.util.Map;
import javax.inject.Inject;

/**
 * {@link AeadCipher} provider for sensitive data storage encryption.
 */
public class DataStorageAeadCipherProvider extends AbstractAeadCipherProvider {

  @Inject
  public DataStorageAeadCipherProvider(
      AeadCipherCryptorExtensionLoader aeadCipherCryptorExtensionLoader,
      CConfiguration cConf, SConfiguration sConf) {
    super(aeadCipherCryptorExtensionLoader, cConf, sConf);
  }

  @Override
  protected String getCipherName() {
    return cConf.get(Encryption.DATA_STORAGE_ENCRYPTION_CIPHER_NAME);
  }

  @Override
  protected Map<String, String> getProperties() {
    return cConf.getPropsWithPrefix(Encryption.DATA_STORAGE_ENCRYPTION_PROPERTIES_PREFIX);
  }

  @Override
  protected Map<String, String> getSecureProperties() {
    return sConf.getPropsWithPrefix(Encryption.DATA_STORAGE_ENCRYPTION_PROPERTIES_PREFIX);
  }
}
