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

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.security.store.SecureStoreMetadata;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A dummy class that is loaded when the user has set the provider to "kms" but the cluster does not
 * have the required libraries. All operations on this class throw an UnsupportedOperationException.
 */
public class DummyKMSStore implements SecureStore, SecureStoreManager {

  private static final String UNSUPPORTED_ERROR_MSG = "Installed version of Hadoop does not support KMS.";

  @Override
  public List<SecureStoreMetadata> listSecureData(String namespace) throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MSG);
  }

  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MSG);
  }

  @Override
  public void putSecureData(String namespace, String name, byte[] data, String description,
                            Map<String, String> properties) throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MSG);
  }

  @Override
  public void deleteSecureData(String namespace, String name) throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MSG);
  }
}
