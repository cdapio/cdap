/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;

/**
 * Provides an abstract implementation for secure store along with some common functionality.
 */
public abstract class AbstractSecureStore {
  /** Separator between the namespace name and the key name */
  static final String NAME_SEPARATOR = ":";

  private static final String KMS_BACKED = "kms";

  static String getKeyName(final String namespace, final String name) {
    return namespace + NAME_SEPARATOR + name;
  }

  public static boolean isKMSBacked(final CConfiguration cConf) {
    return AbstractSecureStore.KMS_BACKED.equalsIgnoreCase(cConf.get(Constants.Security.Store.PROVIDER));
  }

  public static boolean isKMSCapable() {
    try {
      // Check if required KMS classes are present.
      Class.forName("org.apache.hadoop.crypto.key.kms.KMSClientProvider");
      return true;
    } catch (ClassNotFoundException ex) {
      // KMS is not supported.
      return false;
    }
  }
}
