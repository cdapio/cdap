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

package co.cask.cdap.security;

import co.cask.cdap.api.security.store.SecureStore;
import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.io.crypto.KeyProvider;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Helper class for getting KMS security delegation token.
 */
class KMSTokenUtils {
  private static final Logger LOG = LoggerFactory.getLogger(KMSTokenUtils.class);

  static Credentials obtainToken(final SecureStore secureStore, Credentials credentials) {
    try {
      String renewer = UserGroupInformation.getCurrentUser().getShortUserName();
      KeyProvider keyProvider = secureStore.getProvider();
      KeyProviderDelegationTokenExtension keyProviderDelegationTokenExtension =
        KeyProviderDelegationTokenExtension.
          createKeyProviderDelegationTokenExtension(keyProvider);
      keyProviderDelegationTokenExtension.addDelegationTokens(renewer, credentials);
    } catch (IOException e) {
      LOG.error("Failed to get secure token for KMS.", e);
      throw Throwables.propagate(e);
    }

    return credentials;
  }
}
