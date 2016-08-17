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
 * Utility class for secure store.
 */
public class SecureStoreUtils {
  private static final String KMS_BACKED = "kms";
  private static final String FILE_BACKED = "file";

  public static boolean isKMSBacked(final CConfiguration cConf) {
    return KMS_BACKED.equalsIgnoreCase(cConf.get(Constants.Security.Store.PROVIDER));
  }

  public static boolean isFileBacked(final CConfiguration cConf) {
    return FILE_BACKED.equalsIgnoreCase(cConf.get(Constants.Security.Store.PROVIDER));
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

  public static Class<?> getKMSSecureStore() {
    try {
      return Class.forName("co.cask.cdap.security.store.KMSSecureStore");
    } catch (ClassNotFoundException e) {
      // KMSSecureStore could not be loaded
      throw new RuntimeException("CDAP KMS classes could not be loaded. " +
                                   "Please verify that CDAP is correctly installed");
    }
  }
}
