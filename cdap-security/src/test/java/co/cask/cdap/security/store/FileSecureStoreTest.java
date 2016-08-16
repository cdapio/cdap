/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.namespace.InMemoryNamespaceClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.Charsets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FileSecureStoreTest {

  private static final String NAMESPACE1 = "default";
  private static final String NAMESPACE2 = "namespace2";
  private static final String STORE_PATH = System.getProperty("java.io.tmpdir");
  private static final String KEY1 = "key1";
  private static final String VALUE1 = "value1";
  private static final String DESCRIPTION1 = "This is the first key.";
  private static final Map<String, String> PROPERTIES_1 = new HashMap<>();

  private static final String KEY2 = "key2";
  private static final String VALUE2 = "value2";
  private static final String DESCRIPTION2 = "This is the second key.";

  static {
    PROPERTIES_1.put("Prop1", "Val1");
  }

  private static final Map<String, String> PROPERTIES_2 = new HashMap<>();

  static {
    PROPERTIES_2.put("Prop2", "Val2");
  }

  private SecureStoreManager secureStoreManager;
  private SecureStore secureStore;

  @Before
  public void setUp() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.Security.Store.FILE_PATH, STORE_PATH);
    SConfiguration sConf = SConfiguration.create();
    sConf.set(Constants.Security.Store.FILE_PASSWORD, "secret");
    InMemoryNamespaceClient namespaceClient = new InMemoryNamespaceClient();
    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder()
      .setName(new Id.Namespace(NAMESPACE1))
      .build();
    namespaceClient.create(namespaceMeta);
    namespaceMeta = new NamespaceMeta.Builder()
      .setName(new Id.Namespace(NAMESPACE2))
      .build();
    namespaceClient.create(namespaceMeta);
    FileSecureStore fileSecureStore = new FileSecureStore(conf, sConf, namespaceClient);
    secureStoreManager = fileSecureStore;
    secureStore = fileSecureStore;
  }

  @After
  public void tearDown() throws IOException {
    Files.deleteIfExists(Paths.get(STORE_PATH, "securestore"));
  }

  private void populateStore() throws Exception {
    secureStoreManager.putSecureData(NAMESPACE1, KEY1, VALUE1, DESCRIPTION1, PROPERTIES_1);
    secureStoreManager.putSecureData(NAMESPACE1, KEY2, VALUE2, DESCRIPTION2, PROPERTIES_2);
  }

  @Test
  public void testListEmpty() throws Exception {
    Assert.assertEquals(Collections.<String, String>emptyMap(), secureStore.listSecureData(NAMESPACE1));
  }

  @Test
  public void testList() throws Exception {
    populateStore();
    Map<String, String> expected = ImmutableMap.of(KEY1, DESCRIPTION1, KEY2, DESCRIPTION2);
    Assert.assertEquals(expected, secureStore.listSecureData(NAMESPACE1));
  }

  @Test
  public void testGet() throws Exception {
    populateStore();
    SecureStoreMetadata metadata = SecureStoreMetadata.of(KEY1, DESCRIPTION1, PROPERTIES_1);
    SecureStoreData secureStoreData = new SecureStoreData(metadata, VALUE1.getBytes(Charsets.UTF_8));
    Assert.assertArrayEquals(secureStoreData.get(), secureStore.getSecureData(NAMESPACE1, KEY1).get());
    Assert.assertEquals(metadata.getDescription(),
                        secureStore.getSecureData(NAMESPACE1, KEY1).getMetadata().getDescription());
    Assert.assertEquals(metadata.getName(), secureStore.getSecureData(NAMESPACE1, KEY1).getMetadata().getName());
  }

  @Test
  public void testGetMetadata() throws Exception {
    populateStore();
    SecureStoreMetadata metadata = SecureStoreMetadata.of(KEY1, DESCRIPTION1, PROPERTIES_1);
    Assert.assertEquals(metadata.getDescription(),
                        secureStore.getSecureData(NAMESPACE1, KEY1).getMetadata().getDescription());
    Assert.assertEquals(metadata.getName(), secureStore.getSecureData(NAMESPACE1, KEY1).getMetadata().getName());
    SecureStoreMetadata metadata2 = SecureStoreMetadata.of(KEY2, DESCRIPTION2, PROPERTIES_2);
    Assert.assertEquals(metadata2.getDescription(),
                        secureStore.getSecureData(NAMESPACE1, KEY2).getMetadata().getDescription());
    Assert.assertEquals(metadata2.getName(), secureStore.getSecureData(NAMESPACE1, KEY2).getMetadata().getName());
  }

  @Test(expected = Exception.class)
  public void testOverwrite() throws Exception {
    secureStoreManager.putSecureData(NAMESPACE1, KEY1, VALUE1, DESCRIPTION1, PROPERTIES_1);
    SecureStoreData oldData = secureStore.getSecureData(NAMESPACE1, KEY1);
    Assert.assertArrayEquals(VALUE1.getBytes(Charsets.UTF_8), oldData.get());
    String newVal = "New value";
    secureStoreManager.putSecureData(NAMESPACE1, KEY1, newVal, DESCRIPTION1, PROPERTIES_1);
  }

  @Test(expected = NotFoundException.class)
  public void testGetNonExistent() throws Exception {
    secureStore.getSecureData(NAMESPACE1, "Dummy");
  }

  @Test(expected = NotFoundException.class)
  public void testDelete() throws Exception {
    populateStore();
    SecureStoreMetadata metadata = SecureStoreMetadata.of(KEY1, DESCRIPTION1, PROPERTIES_1);
    SecureStoreData secureStoreData = new SecureStoreData(metadata, VALUE1.getBytes(Charsets.UTF_8));
    Assert.assertArrayEquals(secureStoreData.get(), secureStore.getSecureData(NAMESPACE1, KEY1).get());
    secureStoreManager.deleteSecureData(NAMESPACE1, KEY1);
    try {
      secureStore.getSecureData(NAMESPACE1, KEY1);
    } catch (IOException ioe) {
      Assert.assertTrue(ioe.getMessage().contains("not found in the secure store"));
      throw ioe;
    }
  }

  @Test
  public void testMultipleNamespaces() throws Exception {
    populateStore();
    String ns = "namespace2";
    secureStoreManager.putSecureData(ns, KEY1, VALUE1, DESCRIPTION1, PROPERTIES_1);
    Map<String, String> expected = ImmutableMap.of(KEY1, DESCRIPTION1, KEY2, DESCRIPTION2);
    Assert.assertEquals(expected, secureStore.listSecureData(NAMESPACE1));
    Assert.assertNotEquals(expected, secureStore.listSecureData(NAMESPACE2));
    Map<String, String> expected2 = ImmutableMap.of(KEY1, DESCRIPTION1);
    Assert.assertEquals(expected2, secureStore.listSecureData(NAMESPACE2));
    Assert.assertNotEquals(expected2, secureStore.listSecureData(NAMESPACE1));
  }
}
