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

import co.cask.cdap.securestore.spi.SecretAlreadyExistsException;
import co.cask.cdap.securestore.spi.SecretManagerContext;
import co.cask.cdap.securestore.spi.SecretNotFoundException;
import co.cask.cdap.securestore.spi.SecretStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default implementation of {@link SecretManagerContext}.
 *
 */
public class DefaultSecretManagercontext implements SecretManagerContext {
  @Override
  public Map<String, String> getProperties() {
    return Collections.emptyMap();
  }

  @Override
  public SecretStore getSecretsMetadataStore() {
    // TODO CDAP-14699 provide actual store implementation instead of mock in memory store
    return new SecretStore() {
      private static final String SEPARATOR = ":";
      // In memory map to store metadata for secrets
      private final Map<String, byte[]> inMemoryDataStore = new HashMap<>();

      @Override
      public byte[] get(String namespace, String name) {
        String key = getKey(namespace, name);
        if (!exists(key)) {
          throw new SecretNotFoundException(namespace, name);
        }
        return inMemoryDataStore.get(key);
      }

      @Override
      public void store(String namespace, String name, byte[] data) {
        String key = getKey(namespace, name);
        if (exists(key)) {
          throw new SecretAlreadyExistsException(namespace, name);
        }
        inMemoryDataStore.put(key, data);
      }

      @Override
      public Collection<byte[]> list(String namespace) {
        List<byte[]> list = new ArrayList<>();
        for (Map.Entry<String, byte[]> entry : inMemoryDataStore.entrySet()) {
          String[] splitted = entry.getKey().split(":");
          if (splitted[0].equals(namespace)) {
            list.add(entry.getValue());
          }
        }
        return list;
      }

      @Override
      public void delete(String namespace, String name) {
        String key = getKey(namespace, name);
        if (!exists(key)) {
          throw new SecretNotFoundException(namespace, name);
        }
        inMemoryDataStore.remove(key);
      }

      private String getKey(String namespace, String name) {
        return namespace + SEPARATOR + name;
      }


      private boolean exists(String key) {
        return inMemoryDataStore.containsKey(key);
      }
    };
  }
}
