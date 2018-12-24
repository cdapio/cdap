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

package co.cask.cdap.security.store;

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.securestore.spi.SecureData;
import co.cask.cdap.securestore.spi.SecureDataManager;
import co.cask.cdap.securestore.spi.SecureDataManagerContext;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * TODO Check existence of namespace before performing any operations.
 */
@Singleton
public class WrappedSecureStore implements SecureStore, SecureStoreManager {
  private SecureDataManager secureDataManager;

  @Inject
  public WrappedSecureStore(CConfiguration cConf) throws Exception {
    SecureStoreExtensionLoader secureStoreExtensionLoader = new SecureStoreExtensionLoader(cConf);
    Map<String, SecureDataManager> all = secureStoreExtensionLoader.getAll();

    secureDataManager = secureStoreExtensionLoader.getAll().get("cloudkms");
    secureDataManager.initialize(HashMap::new);

    // get secure data manager from the classloader
//    for (Map.Entry<String, SecureDataManager> entry : all.entrySet()) {
//      if (entry.getKey().equals("cloudkms")) {
//        secureDataManager = entry.getValue();
//        secureDataManager.initialize(HashMap::new);
//        break;
//      }
//    }
  }

  /**
   *
   * @param namespace The namespace that this key belongs to.
   * @return
   * @throws Exception
   */
  @Override
  public Map<String, String> listSecureData(String namespace) throws Exception {
    Map<String, String> map = new HashMap<>();
    for (SecureData data : secureDataManager.getSecureData(namespace)) {
      map.put(data.getMetadata().getName(), data.getMetadata().getDescription());
    }

    return map;
  }

  /**
   *
   * @param namespace The namespace that this key belongs to.
   * @param name Name of the data element.
   * @return
   * @throws Exception
   */
  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws Exception {
    Optional<SecureData> data = secureDataManager.getSecureData(namespace, name);
    if (data.isPresent()) {
      SecureData secureData = data.get();
      return new SecureStoreData(new SecureStoreMetadata(secureData.getMetadata().getName(),
                                                         secureData.getMetadata().getDescription(),
                                                         secureData.getMetadata().getCreateTimeMs(),
                                                         secureData.getMetadata().getProperties()),
                                 secureData.getData());
    }

    throw new Exception("Secure Data not found.");
  }

  /**
   *
   * @param namespace The namespace that this key belongs to.
   * @param name This is the identifier that will be used to retrieve this element.
   * @param data The sensitive data that has to be securely stored
   * @param description User provided description of the entry.
   * @param properties associated with this element.
   * @throws Exception
   */
  @Override
  public void putSecureData(String namespace, String name, String data, String description,
                            Map<String, String> properties) throws Exception {
    secureDataManager.storeSecureData(namespace, name, data.getBytes(), description, properties);
  }

  /**
   *
   * @param namespace The namespace that this key belongs to.
   * @param name of the element to delete.
   * @throws Exception
   */
  @Override
  public void deleteSecureData(String namespace, String name) throws Exception {
    secureDataManager.deleteSecureData(namespace, name);
  }
}
