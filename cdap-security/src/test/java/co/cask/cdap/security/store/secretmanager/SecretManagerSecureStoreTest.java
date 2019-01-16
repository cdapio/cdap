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

package co.cask.cdap.security.store.secretmanager;

import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.common.SecureKeyNotFoundException;
import co.cask.cdap.common.namespace.InMemoryNamespaceAdmin;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.security.store.SecureStoreService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link SecretManagerSecureStoreService}.
 */
public class SecretManagerSecureStoreTest {
  private static final String NAMESPACE1 = "namespace1";
  private static SecureStoreService secureStoreService;

  @BeforeClass
  public static void setUp() throws Exception {
    InMemoryNamespaceAdmin namespaceClient = new InMemoryNamespaceAdmin();
    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder()
      .setName(NAMESPACE1)
      .build();
    namespaceClient.create(namespaceMeta);
    secureStoreService = new SecretManagerSecureStoreService(namespaceClient, new MockSecretManagerContext(),
                                                             "mock", new MockSecretManager());
    secureStoreService.startAndWait();
  }

  @AfterClass
  public static void cleanUp() {
    secureStoreService.stopAndWait();
  }

  @Test
  public void testExtensionSecureStore() throws Exception {
    String key1 = "key1";
    String key2 = "key2";
    String value1 = "value1";
    String value2 = "value2";
    String description1 = "description1";
    String description2 = "description2";

    Map<String, String> properties = new HashMap<>();
    properties.put("prop1", "value1");

    // put key value to secure store
    secureStoreService.putSecureData(NAMESPACE1, key1, value1, description1, properties);
    secureStoreService.putSecureData(NAMESPACE1, key2, value2, description2, properties);

    // get key value from secure store
    SecureStoreData ns1Key1 = secureStoreService.getSecureData(NAMESPACE1, key1);
    SecureStoreData ns1Key2 = secureStoreService.getSecureData(NAMESPACE1, key2);

    Assert.assertEquals(key1, ns1Key1.getMetadata().getName());
    Assert.assertArrayEquals(value1.getBytes(StandardCharsets.UTF_8), ns1Key1.get());
    Assert.assertEquals(description1, ns1Key1.getMetadata().getDescription());
    Assert.assertEquals(properties.size(), ns1Key1.getMetadata().getProperties().size());

    Assert.assertEquals(key2, ns1Key2.getMetadata().getName());
    Assert.assertArrayEquals(value2.getBytes(StandardCharsets.UTF_8), ns1Key2.get());
    Assert.assertEquals(description2, ns1Key2.getMetadata().getDescription());
    Assert.assertEquals(properties.size(), ns1Key2.getMetadata().getProperties().size());

    // list key value from secure store
    int i = 1;
    for (Map.Entry<String, String> entry : secureStoreService.listSecureData(NAMESPACE1).entrySet()) {
      Assert.assertEquals("key" + i, entry.getKey());
      Assert.assertEquals("description" + i, entry.getValue());
      i++;
    }

    // delete key value from secure store
    secureStoreService.deleteSecureData(NAMESPACE1, key1);
    secureStoreService.deleteSecureData(NAMESPACE1, key2);

    Assert.assertEquals(0, secureStoreService.listSecureData(NAMESPACE1).size());
  }

  @Test(expected = SecureKeyNotFoundException.class)
  public void testKeyNotFound() throws Exception {
    secureStoreService.getSecureData(NAMESPACE1, "nonexistingkey");
  }
}
