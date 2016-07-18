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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Helper class for getting KMS security delegation token.
 */
class KMSTokenUtils {
  private static boolean supportsKMS;

  static Credentials obtainToken(final SecureStore secureStore, Credentials credentials) {
    try {
      String renewer = UserGroupInformation.getCurrentUser().getShortUserName();
      Class store = Class.forName("co.cask.cdap.security.store.KMSSecureStore");
      @SuppressWarnings("unchecked")
      Method obtainToken = store.getMethod("getProvider");
      KeyProvider keyProvider = (KeyProvider) obtainToken.invoke(secureStore);
      Class tokenExtension = Class.forName("org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension");
      @SuppressWarnings("unchecked")
      Method m = tokenExtension.getMethod("createKeyProviderDelegationTokenExtension");
      Object delegationTokenExtension = m.invoke(null, keyProvider);
      @SuppressWarnings("unchecked")
      Method addToken = tokenExtension.getMethod("addDelegationTokens");
      addToken.invoke(delegationTokenExtension, renewer, credentials);
    } catch (IOException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException |
      IllegalAccessException e) {
      throw Throwables.propagate(e);
    }

    return credentials;
  }
}
