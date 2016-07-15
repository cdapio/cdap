/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.security.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 *
 */
public class KMSTokenUtils {
  private static final Logger LOG = LoggerFactory.getLogger(KMSTokenUtils.class);

  public static Credentials obtainToken(Configuration conf, Credentials credentials) {
    conf.set("hadoop.security.authentication", "kerberos");
    conf.set("hadoop.kms.authentication.token.validity", "1");
    conf.set("hadoop.kms.authentication.type", "kerberos");
    conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
    try {
      String renewer = UserGroupInformation.getCurrentUser().getShortUserName();
      KMSSecureStore kmsSecureStore = new KMSSecureStore(conf);
      KeyProvider keyProvider = kmsSecureStore.getProvider();
      LOG.warn("nsquare: Before logging the keys.");
      for (String k : keyProvider.getKeys()) {
        LOG.warn("nsquare: " + k);
      }
      KeyProviderDelegationTokenExtension keyProviderDelegationTokenExtension =
        KeyProviderDelegationTokenExtension.
          createKeyProviderDelegationTokenExtension(keyProvider);
      Token<?>[] kpTokens = keyProviderDelegationTokenExtension.addDelegationTokens(renewer, credentials);
    } catch (IOException | URISyntaxException e) {
      e.printStackTrace();
    }

    return credentials;
  }

  private static <T extends TokenIdentifier> Token<T> castToken(Object obj) {
    return (Token<T>) obj;
  }
}
