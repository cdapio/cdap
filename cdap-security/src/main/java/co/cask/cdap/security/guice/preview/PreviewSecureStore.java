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
package co.cask.cdap.security.guice.preview;

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;

import java.util.Map;

/**
 * Secure store to be used for preview. Reads are delegated but writes should happen in memory
 */
public class PreviewSecureStore implements SecureStore, SecureStoreManager {

  private final SecureStore delegate;

  public PreviewSecureStore(SecureStore delegate) {
    this.delegate = delegate;
  }

  @Override
  public Map<String, String> listSecureData(String namespace) throws Exception {
    return delegate.listSecureData(namespace);
  }

  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws Exception {
    return delegate.getSecureData(namespace, name);
  }

  @Override
  public void putSecureData(String namespace, String name, String data, String description,
                            Map<String, String> properties) throws Exception {
    //TODO put data in in-mempry map
  }

  @Override
  public void deleteSecureData(String namespace, String name) throws Exception {
    // TODO delete the data from in-memory map if its present otherwise it would be no-op since we do not want to
    // delegate it
  }
}
