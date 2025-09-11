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

package io.cdap.cdap.common.encryption.guice;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.Security.Authentication;
import io.cdap.cdap.common.conf.Constants.Security.Encryption;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.encryption.AeadCipher;
import java.util.Map;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AeadCipher} provider for user credential encryption.
 */
public class UserCredentialAeadCipherProvider extends AbstractAeadCipherProvider {
  private static final Logger LOG = LoggerFactory.getLogger(UserCredentialAeadCipherProvider.class);

  // These properties are here for backwards compatibility, because Tink used to be in cdap-security
  // rather than an extension.
  private static final String TINK_CLEARTEXT_CIPHER_NAME = "tink-cleartext";
  private static final String TINK_CLEARTEXT_KEYSET_KEY = "tink.cleartext.keyset";


  @Inject
  public UserCredentialAeadCipherProvider(
      AeadCipherCryptorExtensionLoader aeadCipherCryptorExtensionLoader, CConfiguration cConf,
      SConfiguration sConf) {
    super(aeadCipherCryptorExtensionLoader, cConf, sConf);
  }

  @Override
  protected String getCipherName() {
    // Backwards compatibility with older properties
    if (sConf.getBoolean(Authentication.USER_CREDENTIAL_ENCRYPTION_ENABLED, false)) {
      LOG.info("sidhdirenge - using tink");
      return TINK_CLEARTEXT_CIPHER_NAME;
    }
    LOG.info("sidhdirenge - cConf.get(Encryption.USER_CREDENTIAL_ENCRYPTION_CIPHER_NAME) : {}",
        cConf.get(Encryption.USER_CREDENTIAL_ENCRYPTION_CIPHER_NAME));
    return cConf.get(Encryption.USER_CREDENTIAL_ENCRYPTION_CIPHER_NAME);
  }

  @Override
  protected Map<String, String> getProperties() {
    LOG.info("sidhdirenge - getProperties {}",
        cConf.getPropsWithPrefix(Encryption.USER_CREDENTIAL_ENCRYPTION_PROPERTIES_PREFIX));
    return cConf.getPropsWithPrefix(Encryption.USER_CREDENTIAL_ENCRYPTION_PROPERTIES_PREFIX);
  }

  @Override
  protected Map<String, String> getSecureProperties() {
    Map<String, String> secureProps = sConf
        .getPropsWithPrefix(Encryption.USER_CREDENTIAL_ENCRYPTION_PROPERTIES_PREFIX);
    // Backwards compatibility with older properties
    if (sConf.getBoolean(Authentication.USER_CREDENTIAL_ENCRYPTION_ENABLED, false)) {
      secureProps.put(TINK_CLEARTEXT_KEYSET_KEY,
          sConf.get(Authentication.USER_CREDENTIAL_ENCRYPTION_KEYSET));
    }
    return secureProps;
  }
}
