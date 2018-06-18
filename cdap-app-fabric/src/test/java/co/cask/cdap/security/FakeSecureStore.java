/*
 * Copyright © 2018 Cask Data, Inc.
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
 *
 */

package co.cask.cdap.security;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreMetadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A fake secure store whose values are stored in memory.
 */
public class FakeSecureStore implements SecureStore {
  private final Map<String, Map<String, SecureStoreData>> values;

  private FakeSecureStore(Map<String, Map<String, SecureStoreData>> values) {
    this.values = values;
  }

  @Override
  public Map<String, String> listSecureData(String namespace) throws Exception {
    Map<String, SecureStoreData> namespaceData = values.get(namespace);
    if (namespaceData == null) {
      throw new Exception("namespace " + namespace + " does not exist");
    }
    // this is an odd API... why doesn't it return Map<String, SecureStoreData>?
    return namespaceData.entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, val -> Bytes.toString(val.getValue().get())));
  }

  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws Exception {
    Map<String, SecureStoreData> namespaceData = values.get(namespace);
    if (namespaceData == null) {
      throw new Exception("namespace " + namespace + " does not exist");
    }
    SecureStoreData data = namespaceData.get(name);
    if (data == null) {
      throw new Exception("Data for name " + name + " does not exist");
    }
    return data;
  }

  /**
   * @return builder to create a fake secure store.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds a {@link FakeSecureStore}.
   */
  public static class Builder {
    private final Map<String, Map<String, SecureStoreData>> values = new HashMap<>();

    public Builder putValue(String namespace, String name, SecureStoreData data) {
      if (!values.containsKey(namespace)) {
        values.put(namespace, new HashMap<>());
      }
      Map<String, SecureStoreData> namespaceValues = values.get(namespace);
      namespaceValues.put(name, data);
      return this;
    }

    public Builder putValue(String namespace, String name, String data) {
      SecureStoreMetadata meta = SecureStoreMetadata.of(name, "desc", Collections.emptyMap());
      return putValue(namespace, name, new SecureStoreData(meta, Bytes.toBytes(data)));
    }

    public FakeSecureStore build() {
      return new FakeSecureStore(values);
    }
  }
}
