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

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.common.SecureKeyNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.namespace.InMemoryNamespaceAdmin;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link DefaultSecureStoreExtensionService}.
 */
public class DefaultSecureStoreExtensionServiceTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final String NAMESPACE1 = "namespace1";

  private static SecureStoreManager secureStoreManager;
  private static SecureStore secureStore;
  private static SecureStoreService extensionService;

  @BeforeClass
  public static void setUp() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Security.Store.FILE_PATH, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.Security.Store.PROVIDER, MockSecretManager.MOCK_SECRET_MANAGER);

    Injector injector = Guice.createInjector(new ConfigModule(cConf),
                                             new AbstractModule() {
                                               @Override
                                               protected void configure() {
                                                 bind(SecureStore.class).to(DefaultSecureStoreExtensionService.class);
                                                 bind(SecureStoreManager.class)
                                                   .to(DefaultSecureStoreExtensionService.class);
                                                 bind(SecureStoreService.class)
                                                   .to(DefaultSecureStoreExtensionService.class);
                                                 bind(SecretManagerProvider.class).to(MockSecretManagerProvider.class);
                                                 bind(NamespaceQueryAdmin.class).to(InMemoryNamespaceAdmin.class)
                                                   .in(Scopes.SINGLETON);
                                               }
                                             });
    secureStoreManager = injector.getInstance(SecureStoreManager.class);
    secureStore = injector.getInstance(SecureStore.class);
    extensionService = injector.getInstance(SecureStoreService.class);
    if (extensionService instanceof Service) {
      ((Service) extensionService).startAndWait();
    }

   NamespaceQueryAdmin namespaceClient = injector.getInstance(NamespaceQueryAdmin.class);
    InMemoryNamespaceAdmin admin = (InMemoryNamespaceAdmin) namespaceClient;
    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder()
      .setName(NAMESPACE1)
      .build();
    admin.create(namespaceMeta);
  }

  @AfterClass
  public static void cleanupClass() {
    if (extensionService instanceof Service) {
      ((Service) extensionService).startAndWait();
    }
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
    secureStoreManager.putSecureData(NAMESPACE1, key1, value1, description1, properties);
    secureStoreManager.putSecureData(NAMESPACE1, key2, value2, description2, properties);

    // get key value from secure store
    SecureStoreData ns1Key1 = secureStore.getSecureData(NAMESPACE1, key1);
    SecureStoreData ns1Key2 = secureStore.getSecureData(NAMESPACE1, key2);

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
    for (Map.Entry<String, String> entry : secureStore.listSecureData(NAMESPACE1).entrySet()) {
      Assert.assertEquals("key" + i, entry.getKey());
      Assert.assertEquals("description" + i, entry.getValue());
      i++;
    }

    // delete key value from secure store
    secureStoreManager.deleteSecureData(NAMESPACE1, key1);
    secureStoreManager.deleteSecureData(NAMESPACE1, key2);

    Assert.assertEquals(0, secureStore.listSecureData(NAMESPACE1).size());
  }

  @Test(expected = SecureKeyNotFoundException.class)
  public void testKeyNotFound() throws Exception {
    secureStore.getSecureData(NAMESPACE1, "nonexistingkey");
  }
}
