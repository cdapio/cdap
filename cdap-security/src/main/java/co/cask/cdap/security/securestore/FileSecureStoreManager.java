/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.security.securestore;

import co.cask.cdap.api.security.SecureStoreManager;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.Map;

/**
 * Provides a write access to the File based secure store.
 */
public class FileSecureStoreManager implements SecureStoreManager {

  private final FileSecureStoreProvider fileSecureStoreProvider;

  @Inject
  FileSecureStoreManager(FileSecureStoreProvider fileSecureStoreProvider) {
    this.fileSecureStoreProvider = fileSecureStoreProvider;
  }

  @Override
  public void put(String name, byte[] data, Map<String, String> properties) throws IOException {
    fileSecureStoreProvider.put(name, data, properties);
  }

  @Override
  public void delete(String name) throws IOException {
    fileSecureStoreProvider.delete(name);
  }
}
