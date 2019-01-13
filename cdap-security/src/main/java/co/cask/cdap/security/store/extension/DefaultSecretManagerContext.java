/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.security.store.extension;

import co.cask.cdap.securestore.spi.SecretManagerContext;
import co.cask.cdap.securestore.spi.SecretNotFoundException;
import co.cask.cdap.securestore.spi.SecretStore;
import co.cask.cdap.securestore.spi.secret.Decoder;
import co.cask.cdap.securestore.spi.secret.Encoder;

import java.io.IOException;
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
public class DefaultSecretManagerContext implements SecretManagerContext {
  @Override
  public Map<String, String> getProperties() {
    return Collections.emptyMap();
  }

  @Override
  public SecretStore getSecretStore() {
    // TODO CDAP-14699 provide actual store implementation instead of mock in memory store
    return new SecretStore() {
      private static final String SEPARATOR = ":";
      // In memory map to store metadata for secrets
      private final Map<String, byte[]> inMemoryDataStore = new HashMap<>();

      @Override
      public <T> T get(String namespace, String name, Decoder<T> decoder) throws SecretNotFoundException, IOException {
        String key = getKey(namespace, name);
        if (!inMemoryDataStore.containsKey(key)) {
          throw new SecretNotFoundException(namespace, name);
        }
        return decoder.decode(inMemoryDataStore.get(key));
      }

      @Override
      public <T> Collection<T> list(String namespace, Decoder<T> decoder) throws IOException {
        List<T> list = new ArrayList<>();
        for (Map.Entry<String, byte[]> entry : inMemoryDataStore.entrySet()) {
          String[] splitted = entry.getKey().split(":");
          if (splitted[0].equals(namespace)) {
            list.add(decoder.decode(entry.getValue()));
          }
        }
        return list;
      }

      @Override
      public <T> void store(String namespace, String name, Encoder<T> encoder, T data) throws IOException {
        String key = getKey(namespace, name);
        inMemoryDataStore.put(key, encoder.encode(data));
      }

      @Override
      public void delete(String namespace, String name) throws SecretNotFoundException, IOException {
        String key = getKey(namespace, name);
        if (!inMemoryDataStore.containsKey(key)) {
          throw new SecretNotFoundException(name, name);
        }
        inMemoryDataStore.remove(key);
      }

      private String getKey(String namespace, String name) {
        return namespace + SEPARATOR + name;
      }
    };
  }
}
