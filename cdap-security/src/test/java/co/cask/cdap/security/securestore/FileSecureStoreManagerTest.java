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

import co.cask.cdap.api.security.SecureStore;
import co.cask.cdap.api.security.SecureStoreManager;
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
import java.util.HashMap;
import java.util.Map;

public class FileSecureStoreManagerTest {
  private static final String STORE_PATH = System.getProperty("java.io.tmpdir");
  private static final String DESCRIPTION_KEY = "description";
  private static final String KEY1 = "key1";
  private static final String VALUE1 = "value1";
  private static final String DESCRIPTION1 = "This is the first key.";
  private static final String KEY2 = "key2";
  private static final String VALUE2 = "value2";
  private static final String DESCRIPTION2 = "This is the second key.";

  private FileSecureStoreProvider fileSecureStoreProvider;
  private SecureStoreManager secureStoreManager;
  private SecureStore secureStore;

  @Before
  public void setUp() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.Security.Store.SECURE_STORE_FILE_PATH, STORE_PATH);
    fileSecureStoreProvider = FileSecureStoreProvider.getInstance(conf);
    secureStoreManager = new FileSecureStoreManager(fileSecureStoreProvider);
    secureStore = new FileSecureStore(fileSecureStoreProvider);
  }

  @After
  public void tearDown() throws IOException {
    Files.deleteIfExists(Paths.get(STORE_PATH, "securestore"));
  }

  private void populateStore() throws IOException {
    Map<String, String> properties1 = new HashMap<>(1);
    properties1.put(DESCRIPTION_KEY, DESCRIPTION1);
    Map<String, String> properties2 = new HashMap<>(1);
    properties2.put(DESCRIPTION_KEY, DESCRIPTION2);
    fileSecureStoreProvider.put(KEY1, VALUE1.getBytes(Charsets.UTF_8), properties1);
    fileSecureStoreProvider.put(KEY2, VALUE2.getBytes(Charsets.UTF_8), properties2);
  }

  @Test
  public void put() throws Exception {
    Map<String, String> properties1 = new HashMap<>(1);
    properties1.put(DESCRIPTION_KEY, DESCRIPTION1);
    Map<String, String> properties2 = new HashMap<>(1);
    properties2.put(DESCRIPTION_KEY, DESCRIPTION2);
    secureStoreManager.put(KEY1, VALUE1.getBytes(Charsets.UTF_8), properties1);
    secureStoreManager.put(KEY2, VALUE2.getBytes(Charsets.UTF_8), properties2);
    Map<String, String> expectedList = new HashMap<>(2);
    expectedList.put(KEY1, DESCRIPTION1);
    expectedList.put(KEY2, DESCRIPTION2);
    Assert.assertEquals(expectedList, secureStore.list());
  }

  @Test
  public void delete() throws Exception {

  }
}
