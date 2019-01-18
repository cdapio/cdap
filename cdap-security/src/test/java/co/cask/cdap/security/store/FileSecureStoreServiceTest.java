/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
import co.cask.cdap.common.namespace.InMemoryNamespaceAdmin;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.commons.io.Charsets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileSecureStoreServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final String NAMESPACE1 = "default";
  private static final String NAMESPACE2 = "namespace2";
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
    conf.set(Constants.Security.Store.FILE_PATH, TEMP_FOLDER.newFolder().getAbsolutePath());
    SConfiguration sConf = SConfiguration.create();
    sConf.set(Constants.Security.Store.FILE_PASSWORD, "secret");
    InMemoryNamespaceAdmin namespaceClient = new InMemoryNamespaceAdmin();
    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder()
      .setName(NAMESPACE1)
      .build();
    namespaceClient.create(namespaceMeta);
    namespaceMeta = new NamespaceMeta.Builder()
      .setName(NAMESPACE2)
      .build();
    namespaceClient.create(namespaceMeta);
    FileSecureStoreService fileSecureStoreService = new FileSecureStoreService(conf, sConf, namespaceClient);
    secureStoreManager = fileSecureStoreService;
    secureStore = fileSecureStoreService;
  }

  private void populateStore() throws Exception {
    secureStoreManager.put(NAMESPACE1, KEY1, VALUE1, DESCRIPTION1, PROPERTIES_1);
    secureStoreManager.put(NAMESPACE1, KEY2, VALUE2, DESCRIPTION2, PROPERTIES_2);
  }

  @Test
  public void testListEmpty() throws Exception {
    Assert.assertEquals(Collections.emptyList(), secureStore.list(NAMESPACE1));
  }

  @Test
  public void testList() throws Exception {
    populateStore();
    List<SecureStoreMetadata> metadatas = new ArrayList<>(secureStore.list(NAMESPACE1));
    metadatas.sort(Comparator.comparing(SecureStoreMetadata::getName));
    verifyList(metadatas, ImmutableMap.of(KEY1, DESCRIPTION1, KEY2, DESCRIPTION2));
  }

  @Test
  public void testGet() throws Exception {
    populateStore();
    SecureStoreMetadata metadata = SecureStoreMetadata.of(KEY1, DESCRIPTION1, PROPERTIES_1);
    SecureStoreData secureStoreData = new SecureStoreData(metadata, VALUE1.getBytes(Charsets.UTF_8));
    Assert.assertArrayEquals(secureStoreData.get(), secureStore.get(NAMESPACE1, KEY1).get());
    Assert.assertEquals(metadata.getDescription(),
                        secureStore.get(NAMESPACE1, KEY1).getMetadata().getDescription());
    Assert.assertEquals(metadata.getName(), secureStore.get(NAMESPACE1, KEY1).getMetadata().getName());
  }

  @Test
  public void testGetMetadata() throws Exception {
    populateStore();
    SecureStoreMetadata metadata = SecureStoreMetadata.of(KEY1, DESCRIPTION1, PROPERTIES_1);
    Assert.assertEquals(metadata.getDescription(),
                        secureStore.get(NAMESPACE1, KEY1).getMetadata().getDescription());
    Assert.assertEquals(metadata.getName(), secureStore.get(NAMESPACE1, KEY1).getMetadata().getName());
    SecureStoreMetadata metadata2 = SecureStoreMetadata.of(KEY2, DESCRIPTION2, PROPERTIES_2);
    Assert.assertEquals(metadata2.getDescription(),
                        secureStore.get(NAMESPACE1, KEY2).getMetadata().getDescription());
    Assert.assertEquals(metadata2.getName(), secureStore.get(NAMESPACE1, KEY2).getMetadata().getName());
  }

  @Test
  public void testOverwrite() throws Exception {
    secureStoreManager.put(NAMESPACE1, KEY1, VALUE1, DESCRIPTION1, PROPERTIES_1);
    SecureStoreData oldData = secureStore.get(NAMESPACE1, KEY1);
    Assert.assertArrayEquals(VALUE1.getBytes(Charsets.UTF_8), oldData.get());
    String newVal = "New value";
    secureStoreManager.put(NAMESPACE1, KEY1, newVal, DESCRIPTION2, PROPERTIES_1);

    SecureStoreData updated = secureStore.get(NAMESPACE1, KEY1);
    Assert.assertArrayEquals(newVal.getBytes(StandardCharsets.UTF_8), updated.get());
    Assert.assertEquals(DESCRIPTION2, updated.getMetadata().getDescription());
  }

  @Test(expected = NotFoundException.class)
  public void testGetNonExistent() throws Exception {
    secureStore.get(NAMESPACE1, "Dummy");
  }

  @Test(expected = NotFoundException.class)
  public void testDelete() throws Exception {
    populateStore();
    SecureStoreMetadata metadata = SecureStoreMetadata.of(KEY1, DESCRIPTION1, PROPERTIES_1);
    SecureStoreData secureStoreData = new SecureStoreData(metadata, VALUE1.getBytes(Charsets.UTF_8));
    Assert.assertArrayEquals(secureStoreData.get(), secureStore.get(NAMESPACE1, KEY1).get());
    secureStoreManager.delete(NAMESPACE1, KEY1);
    try {
      secureStore.get(NAMESPACE1, KEY1);
    } catch (IOException ioe) {
      Assert.assertTrue(ioe.getMessage().contains("not found in the secure store"));
      throw ioe;
    }
  }

  @Test
  public void testMultipleNamespaces() throws Exception {
    populateStore();
    secureStoreManager.put(NAMESPACE2, "ns2-" + KEY1, VALUE1, DESCRIPTION1, PROPERTIES_1);
    secureStoreManager.put(NAMESPACE2, "ns2-" + KEY2, VALUE2, DESCRIPTION2, PROPERTIES_2);

    List<SecureStoreMetadata> metadatas = new ArrayList<>(secureStore.list(NAMESPACE1));
    metadatas.sort(Comparator.comparing(SecureStoreMetadata::getName));
    verifyList(metadatas, ImmutableMap.of(KEY1, DESCRIPTION1, KEY2, DESCRIPTION2));

    metadatas = new ArrayList<>(secureStore.list(NAMESPACE2));
    metadatas.sort(Comparator.comparing(SecureStoreMetadata::getName));
    verifyList(metadatas, ImmutableMap.of("ns2-" + KEY1, DESCRIPTION1, "ns2-" + KEY2, DESCRIPTION2));
  }

  private void verifyList(List<SecureStoreMetadata> metadatas, ImmutableMap<String, String> map) {
    Assert.assertEquals(metadatas.size(), map.size());
    UnmodifiableIterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();
    for (SecureStoreMetadata metadata : metadatas) {
      Map.Entry<String, String> expected = iterator.next();
      Assert.assertEquals(expected.getKey(), metadata.getName());
      Assert.assertEquals(expected.getValue(), metadata.getDescription());
    }
  }
}
