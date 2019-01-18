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

package co.cask.cdap.security.store;

import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import com.google.common.util.concurrent.AbstractIdleService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A dummy class that is loaded when the user has set the provider to "none".
 * All operations on this class throw an UnsupportedOperationException.
 */
public class DummySecureStoreService extends AbstractIdleService implements SecureStoreService {

  private static final String SECURE_STORE_SETUP = "Secure store is not configured. To use secure store please set " +
    "\"security.store.provider\" property in cdap-site.xml.";

  @Override
  public List<SecureStoreMetadata> list(String namespace) throws IOException {
    throw new UnsupportedOperationException(SECURE_STORE_SETUP);
  }

  @Override
  public SecureStoreData get(String namespace, String name) throws IOException {
    throw new UnsupportedOperationException(SECURE_STORE_SETUP);
  }

  @Override
  public void put(String namespace, String name, String data, String description,
                  Map<String, String> properties) throws IOException {
    throw new UnsupportedOperationException(SECURE_STORE_SETUP);
  }

  @Override
  public void delete(String namespace, String name) throws IOException {
    throw new UnsupportedOperationException(SECURE_STORE_SETUP);
  }

  @Override
  protected void startUp() throws Exception {
    // no-op
  }

  @Override
  protected void shutDown() throws Exception {
    // no-op
  }
}
