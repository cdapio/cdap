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

package io.cdap.cdap.security.store.secretmanager;

import io.cdap.cdap.securestore.spi.SecretManager;
import io.cdap.cdap.securestore.spi.SecretManagerContext;
import io.cdap.cdap.securestore.spi.SecretManagerInfo;
import io.cdap.cdap.securestore.spi.SecretNotFoundException;
import io.cdap.cdap.securestore.spi.secret.Secret;
import io.cdap.cdap.securestore.spi.secret.SecretMetadata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Mock Secret Manager for unit tests.
 */
public class MockSecretManager implements SecretManager {
  private static final String MOCK_SECRET_MANAGER = "mock_secret_mananger";
  private static final String SEPARATOR = ":";
  private Map<String, Secret> map;
  private Set<String> leasedKeys;

  @Override
  public String getName() {
    return MOCK_SECRET_MANAGER;
  }

  @Override
  public void initialize(SecretManagerContext context) throws IOException {
    map = new HashMap<>();
    leasedKeys = new HashSet<>();
  }

  @Override
  public void store(String namespace, Secret secret) throws IOException {
    String key = getKey(namespace, secret.getMetadata().getName());
    map.put(key, secret);
  }

  @Override
  public Secret get(String namespace, String name) throws SecretNotFoundException, IOException {
    String key = getKey(namespace, name);
    if (!map.containsKey(key)) {
      throw new SecretNotFoundException(namespace, name);
    }
    return map.get(key);
  }

  @Override
  public Collection<SecretMetadata> list(String namespace) throws IOException {
    List<SecretMetadata> list = new ArrayList<>();
    for (Map.Entry<String, Secret> entry : map.entrySet()) {
      String[] splitted = entry.getKey().split(":");
      if (splitted[0].equals(namespace)) {
        list.add(entry.getValue().getMetadata());
      }
    }
    return list;
  }

  @Override
  public void delete(String namespace, String name) throws SecretNotFoundException, IOException {
    String key = getKey(namespace, name);
    if (!map.containsKey(key)) {
      throw new SecretNotFoundException(name, name);
    }
    map.remove(key);
  }

  @Override
  public void destroy(SecretManagerContext context) {
    map.clear();
    leasedKeys.clear();
  }

  @Override
  public SecretManagerInfo getStoreInfo() {
    return new SecretManagerInfo(EnumSet.of(SecretManagerInfo.Capability.SECRET_LEASING));
  }

  @Override
  public boolean acquireLease(String namespace, String key, long timeoutMs, String leaseHolder)
      throws IOException {
    String fullKey = getKey(namespace, key);
    if (!map.containsKey(fullKey)) {
      throw new IOException("Not found");
    }
    return leasedKeys.add(fullKey);
  }

  @Override
  public boolean releaseLease(String namespace, String key, String leaseHolder) throws IOException {
    String fullKey = getKey(namespace, key);
    if (!map.containsKey(fullKey)) {
      throw new IOException("Not found");
    }
    return leasedKeys.remove(fullKey);
  }

  private String getKey(String namespace, String name) {
    return namespace + SEPARATOR + name;
  }
}
