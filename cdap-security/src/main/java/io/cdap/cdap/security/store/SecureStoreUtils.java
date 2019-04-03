/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.security.store;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;

/**
 * Utility class for secure store.
 */
public class SecureStoreUtils {
  private static final String KMS_BACKED = "kms";
  private static final String FILE_BACKED = "file";
  private static final String NONE = "none";
  private static final String KMS_CLASS_NAME = "io.cdap.cdap.security.store.KMSSecureStoreService";

  public static boolean isKMSBacked(final CConfiguration cConf) {
    return KMS_BACKED.equalsIgnoreCase(cConf.get(Constants.Security.Store.PROVIDER));
  }

  public static boolean isFileBacked(final CConfiguration cConf) {
    return FILE_BACKED.equalsIgnoreCase(cConf.get(Constants.Security.Store.PROVIDER));
  }

  /**
   * Checks if the store provider is none. Returns true if the provider value is set to none.
   */
  public static boolean isNone(final CConfiguration cConf) {
    return NONE.equalsIgnoreCase(cConf.get(Constants.Security.Store.PROVIDER));
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

  @SuppressWarnings("unchecked")
  public static Class<? extends SecureStoreService> getKMSSecureStore() {
    try {
      // we know that the class is a SecureStoreService
      return (Class<? extends SecureStoreService>) Class.forName(KMS_CLASS_NAME);
    } catch (ClassNotFoundException e) {
      // KMSSecureStore could not be loaded
      throw new RuntimeException("CDAP KMS classes could not be loaded. " +
                                   "Please verify that CDAP is correctly installed");
    }
  }
}
