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

package co.cask.cdap.securestore.spi;

import co.cask.cdap.securestore.spi.secret.Secret;

import java.util.HashMap;
import java.util.Map;

/**
 * Mock secret metadata store.
 *
 * TODO CDAP-14699 Remove this class once dataset is exposed through SecretsManager context.
 */
public class MockSecretsMetadataStore implements SecretsMetadataStore<Secret> {
  private static final String SEPARATOR = ":";
  private final Map<String, Secret> inMemoryDataStore;

  public MockSecretsMetadataStore() {
    this.inMemoryDataStore = new HashMap<>();
  }

  @Override
  public Secret get(String namespace, String name) throws Exception {
    String key = getKey(namespace, name);
    if (!exists(key)) {
      throw new SecretNotFoundException(String.format("Secret %s not found in the namespace %s. Please provide " +
                                                        "correct secret name.",
                                                      name, namespace));
    }
    return inMemoryDataStore.get(key);
  }

  @Override
  public void store(String namespace, String name, Secret data) throws Exception {
    String key = getKey(namespace, name);
    if (exists(key)) {
      throw new SecretAlreadyExistsException(String.format("Secret %s already exists in the namespace %s. Please " +
                                                             "provide another name for the secret.",
                                                           name, namespace));
    }
    inMemoryDataStore.put(key, data);
  }

  @Override
  public Map<String, String> list(String namespace) throws Exception {
    Map<String, String> map = new HashMap<>();
    for (Map.Entry<String, Secret> entry : inMemoryDataStore.entrySet()) {
      String[] splitted = entry.getKey().split(":");
      if (splitted[0].equals(namespace)) {
        map.put(entry.getValue().getMetadata().getName(), entry.getValue().getMetadata().getDescription());
      }
    }
    return map;
  }

  @Override
  public void delete(String namespace, String name) throws Exception {
    String key = getKey(namespace, name);
    if (!exists(key)) {
      throw new SecretNotFoundException(String.format("Secret %s not found in the namespace %s. Please provide " +
                                                        "correct secret name.",
                                                      name, namespace));
    }
    inMemoryDataStore.remove(key);
  }

  private String getKey(String namespace, String name) {
    return namespace + SEPARATOR + name;
  }


  private boolean exists(String key) {
    return inMemoryDataStore.containsKey(key);
  }
}
