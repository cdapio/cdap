/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.cdap.securestore;

import co.cask.cdap.securestore.spi.SecureData;
import co.cask.cdap.securestore.spi.SecureDataStore;
import co.cask.cdap.securestore.spi.SecureDataStoreContext;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class CloudKMS implements SecureDataStore {
  @Override
  public String getType() {
    return "cloud_kms";
  }

  @Override
  public void initialize(SecureDataStoreContext context) throws Exception {

  }

  @Override
  public void storeSecureData(String namespace, String name, byte[] data, String description,
                              Map<String, String> properties) throws Exception {

  }

  @Override
  public Optional<SecureData> getSecureData(String namespace, String name) throws Exception {
    return Optional.empty();
  }

  @Override
  public Collection<SecureData> getSecureData(String namespace) throws Exception {
    return null;
  }

  @Override
  public void deleteSecureData(String namespace, String name) throws Exception {

  }
}
