/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.security;

import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.proto.security.SecureStoreCreateRequest;
import co.cask.cdap.security.store.FileSecureStore;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SecureStoreTest extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static Type listType = new TypeToken<ArrayList<FileSecureStore.SecureStoreListEntry>>() { }.getType();
  private static final String KEY = "Key1";
  private static final String DESCRIPTION = "This is Key1";
  private static final String DATA = "Secret1";
  private static final Map<String, String> PROPERTIES = new HashMap<>();
  static {
    PROPERTIES.put("Prop1", "Val1");
    PROPERTIES.put("Prop2", "Val2");
  }

  private static final String KEY2 = "Key2";
  private static final String DESCRIPTION2 = "This is Key2";
  private static final String DATA2 = "Secret2";
  private static final Map<String, String> PROPERTIES2 = new HashMap<>();
  static {
    PROPERTIES2.put("Prop1", "Val1");
    PROPERTIES2.put("Prop2", "Val2");
  }

  @Test
  public void testCreate() throws Exception {
    SecureStoreCreateRequest secureStoreCreateRequest = new SecureStoreCreateRequest(KEY, DESCRIPTION, DATA,
                                                                                     PROPERTIES);
    HttpResponse response = doPut("/v3/security/store/namespaces/default/key", GSON.toJson(secureStoreCreateRequest));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = delete(KEY);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testGet() throws Exception {
    SecureStoreCreateRequest secureStoreCreateRequest = new SecureStoreCreateRequest(KEY, DESCRIPTION, DATA,
                                                                                     PROPERTIES);
    HttpResponse response = doPut("/v3/security/store/namespaces/default/key", GSON.toJson(secureStoreCreateRequest));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet("/v3/security/store/namespaces/default/keys/" + KEY);
    Assert.assertEquals('"' + DATA + '"', readResponse(response));

    // Get again
    response = doGet("/v3/security/store/namespaces/default/keys/" + KEY);
    Assert.assertEquals('"' + DATA + '"', readResponse(response));

    response = delete(KEY);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testList() throws Exception {
    HttpResponse response = doGet("/v3/security/store/namespaces/default/keys/");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals("[]", readResponse(response));
    SecureStoreCreateRequest secureStoreCreateRequest = new SecureStoreCreateRequest(KEY, DESCRIPTION, DATA,
                                                                                     PROPERTIES);
    response = doPut("/v3/security/store/namespaces/default/key", GSON.toJson(secureStoreCreateRequest));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet("/v3/security/store/namespaces/default/keys/");
    String result = readResponse(response);
    List<FileSecureStore.SecureStoreListEntry> expectedList = new ArrayList<>();
    expectedList.add(new FileSecureStore.SecureStoreListEntry(KEY, DESCRIPTION));
    List<FileSecureStore.SecureStoreListEntry> list = new Gson().fromJson(result, listType);
    for (FileSecureStore.SecureStoreListEntry entry : list) {
      Assert.assertTrue(expectedList.contains(entry));
    }

    secureStoreCreateRequest = new SecureStoreCreateRequest(KEY2, DESCRIPTION2, DATA2, PROPERTIES2);
    response = doPut("/v3/security/store/namespaces/default/key", GSON.toJson(secureStoreCreateRequest));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet("/v3/security/store/namespaces/default/keys/");
    String result2 = readResponse(response);
    list = new Gson().fromJson(result2, listType);
    expectedList.add(new FileSecureStore.SecureStoreListEntry(KEY2, DESCRIPTION2));
    for (FileSecureStore.SecureStoreListEntry entry : list) {
      Assert.assertTrue(expectedList.contains(entry));
    }

    delete(KEY);
    response = doGet("/v3/security/store/namespaces/default/keys/");
    String result3 = readResponse(response);
    list = new Gson().fromJson(result3, listType);
    expectedList.remove(new FileSecureStore.SecureStoreListEntry(KEY, DESCRIPTION));
    for (FileSecureStore.SecureStoreListEntry entry : list) {
      Assert.assertTrue(expectedList.contains(entry));
    }
  }

  public HttpResponse delete(String key) throws Exception {
    return doDelete("/v3/security/store/namespaces/default/keys/" + key);
  }

}
