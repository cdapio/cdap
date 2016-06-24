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

package co.cask.cdap.security.securestore;

import co.cask.cdap.api.security.securestore.SecureStore;
import co.cask.cdap.api.security.securestore.SecureStoreData;
import co.cask.cdap.api.security.securestore.SecureStoreManager;
import co.cask.cdap.api.security.securestore.SecureStoreMetadata;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import org.apache.commons.io.Charsets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileSecureStoreTest {

  private static final String STORE_PATH = System.getProperty("java.io.tmpdir");
  private static final String DESCRIPTION_KEY = "description";
  private static final String KEY1 = "key1";
  private static final String VALUE1 = "value1";
  private static final String DESCRIPTION1 = "This is the first key.";
  private static final Map<String, String> PROPERTIES_1 = new HashMap<>();

  private static final String KEY2 = "key2";
  private static final String VALUE2 = "value2";
  private static final String DESCRIPTION2 = "This is the second key.";

  static {
    PROPERTIES_1.put(DESCRIPTION_KEY, DESCRIPTION1);
  }

  private static final Map<String, String> PROPERTIES_2 = new HashMap<>();

  static {
    PROPERTIES_2.put(DESCRIPTION_KEY, DESCRIPTION2);
  }

  private SecureStoreManager secureStoreManager;
  private SecureStore secureStore;

  @Before
  public void setUp() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.Security.Store.FILE_PATH, STORE_PATH);
    FileSecureStore fileSecureStore = new FileSecureStore(conf);
    secureStoreManager = fileSecureStore;
    secureStore = fileSecureStore;
  }

  @After
  public void tearDown() throws IOException {
    Files.deleteIfExists(Paths.get(STORE_PATH, "securestore"));
  }

  private void populateStore() throws IOException {
    secureStoreManager.put(KEY1, VALUE1.getBytes(Charsets.UTF_8), PROPERTIES_1);
    secureStoreManager.put(KEY2, VALUE2.getBytes(Charsets.UTF_8), PROPERTIES_2);
  }

  @Test
  public void testListEmpty() throws IOException {
    Assert.assertEquals(new ArrayList<>(), secureStore.list());
  }

  @Test
  public void testList() throws IOException {
    populateStore();
    List<SecureStoreMetadata> expectedList = new ArrayList<>();
    expectedList.add(secureStore.get(KEY2).getMetadata());
    expectedList.add(secureStore.get(KEY1).getMetadata());
    Assert.assertEquals(expectedList, secureStore.list());
  }

  @Test
  public void testGet() throws IOException {
    populateStore();
    SecureStoreMetadata metadata = SecureStoreMetadata.of(KEY1, PROPERTIES_1);
    SecureStoreData secureStoreData = new SecureStoreData(metadata, VALUE1.getBytes(Charsets.UTF_8));
    Assert.assertArrayEquals(secureStoreData.get(), secureStore.get(KEY1).get());
    Assert.assertEquals(metadata.getDescription(), secureStore.get(KEY1).getMetadata().getDescription());
    Assert.assertEquals(metadata.getName(), secureStore.get(KEY1).getMetadata().getName());
  }

  @Test
  public void testGetMetadata() throws IOException {
    populateStore();
    SecureStoreMetadata metadata = SecureStoreMetadata.of(KEY1, PROPERTIES_1);
    Assert.assertEquals(metadata.getDescription(), secureStore.get(KEY1).getMetadata().getDescription());
    Assert.assertEquals(metadata.getName(), secureStore.get(KEY1).getMetadata().getName());
    SecureStoreMetadata metadata2 = SecureStoreMetadata.of(KEY2, PROPERTIES_2);
    Assert.assertEquals(metadata2.getDescription(), secureStore.get(KEY2).getMetadata().getDescription());
    Assert.assertEquals(metadata2.getName(), secureStore.get(KEY2).getMetadata().getName());
  }

  @Test
  public void testOverwrite() throws IOException {
    secureStoreManager.put(KEY1, VALUE1.getBytes(Charsets.UTF_8), PROPERTIES_1);
    SecureStoreData oldData = secureStore.get(KEY1);
    long oldCreateTime = oldData.getMetadata().getLastModifiedTime();
    Assert.assertArrayEquals(VALUE1.getBytes(Charsets.UTF_8), oldData.get());
    String newVal = "New value";
    secureStoreManager.put(KEY1, newVal.getBytes(Charsets.UTF_8), PROPERTIES_1);
    long newCreateTime = secureStore.get(KEY1).getMetadata().getLastModifiedTime();
    Assert.assertArrayEquals(newVal.getBytes(Charsets.UTF_8), secureStore.get(KEY1).get());
    Assert.assertNotEquals(oldCreateTime, newCreateTime);
  }

  @Test(expected = IOException.class)
  public void testGet_NonExistent() throws IOException {
    secureStore.get("Dummy");
  }

  @Test(expected = IOException.class)
  public void testDelete() throws IOException {
    populateStore();
    SecureStoreMetadata metadata = SecureStoreMetadata.of(KEY1, PROPERTIES_1);
    SecureStoreData secureStoreData = new SecureStoreData(metadata, VALUE1.getBytes(Charsets.UTF_8));
    Assert.assertArrayEquals(secureStoreData.get(), secureStore.get(KEY1).get());
    secureStoreManager.delete(KEY1);
    try {
      secureStore.get(KEY1);
    } catch (IOException ioe) {
      Assert.assertTrue(ioe.getMessage().contains("not found in the secure store"));
      throw ioe;
    }
  }
}
