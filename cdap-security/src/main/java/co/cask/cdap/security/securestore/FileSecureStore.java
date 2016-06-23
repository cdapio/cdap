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

import co.cask.cdap.api.security.securestore.SecureStore;
import co.cask.cdap.api.security.securestore.SecureStoreData;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.Map;

/**
 * Provides a read only interface to the secure store.
 * For write access use {@link FileSecureStoreManager}
 */
public class FileSecureStore implements SecureStore {

  private final FileSecureStoreProvider fileSecureStoreProvider;

  @Inject
  FileSecureStore(FileSecureStoreProvider fileSecureStoreProvider) {
    this.fileSecureStoreProvider = fileSecureStoreProvider;
  }

  /**
   * @return A map of all the elements stored in the store. The map is Name -> Description.
   */
  @Override
  public Map<String, String> list() throws IOException {
    return fileSecureStoreProvider.list();
  }

  /**
   * @param name Name of the element.
   * @return An object representing the securely stored element associated with the name.
   */
  @Override
  public SecureStoreData get(String name) throws IOException {
    return new FileSecureStoreData(fileSecureStoreProvider.getSecureStoreMetadata(name),
        fileSecureStoreProvider.getData(name));
  }
}
