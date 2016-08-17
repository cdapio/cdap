/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.proto.security.SecureKeyCreateRequest;
import co.cask.cdap.proto.security.SecureKeyListEntry;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SecureStoreTest extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static Type listType = new TypeToken<ArrayList<SecureKeyListEntry>>() { }.getType();
  private static final String KEY = "key1";
  private static final String DESCRIPTION = "This is Key1";
  private static final String DATA = "Secret1";
  private static final Map<String, String> PROPERTIES = ImmutableMap.of("Prop1", "Val1", "Prop2", "Val2");
  private static final String KEY2 = "key2";
  private static final String DESCRIPTION2 = "This is Key2";
  private static final String DATA2 = "Secret2";
  private static final Map<String, String> PROPERTIES2 = ImmutableMap.of("Prop1", "Val1", "Prop2", "Val2");

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration cConf = createBasicCConf();
    cConf.set(Constants.Security.Store.PROVIDER, "file");
    SConfiguration sConf = SConfiguration.create();
    sConf.set(Constants.Security.Store.FILE_PASSWORD, "secret");
    initializeAndStartServices(cConf, sConf);
  }

  @Test
  public void testCreate() throws Exception {
    SecureKeyCreateRequest secureKeyCreateRequest = new SecureKeyCreateRequest(DESCRIPTION, DATA,
                                                                               PROPERTIES);
    HttpResponse response = doPut("/v3/namespaces/default/securekeys/" + KEY,
                                  GSON.toJson(secureKeyCreateRequest));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = delete(KEY);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testGet() throws Exception {
    SecureKeyCreateRequest secureKeyCreateRequest = new SecureKeyCreateRequest(DESCRIPTION, DATA,
                                                                               PROPERTIES);
    HttpResponse response = doPut("/v3/namespaces/default/securekeys/" + KEY,
                                  GSON.toJson(secureKeyCreateRequest));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet("/v3/namespaces/default/securekeys/" + KEY);
    Assert.assertEquals(DATA, readResponse(response));

    // Get again
    response = doGet("/v3/namespaces/default/securekeys/" + KEY);
    Assert.assertEquals(DATA, readResponse(response));

    response = delete(KEY);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testList() throws Exception {
    // Test empty list
    HttpResponse response = doGet("/v3/namespaces/default/securekeys/");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals("[]", readResponse(response));

    // One element
    SecureKeyCreateRequest secureKeyCreateRequest = new SecureKeyCreateRequest(DESCRIPTION, DATA,
                                                                               PROPERTIES);
    response = doPut("/v3/namespaces/default/securekeys/" + KEY, GSON.toJson(secureKeyCreateRequest));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet("/v3/namespaces/default/securekeys/");
    String result = readResponse(response);
    List<SecureKeyListEntry> expected = new ArrayList<>();
    expected.add(new SecureKeyListEntry(KEY, DESCRIPTION));
    List<SecureKeyListEntry> list = GSON.fromJson(result, listType);
    for (SecureKeyListEntry entry : list) {
      Assert.assertTrue(expected.contains(entry));
    }

    // Two elements
    secureKeyCreateRequest = new SecureKeyCreateRequest(DESCRIPTION2, DATA2, PROPERTIES2);
    response = doPut("/v3/namespaces/default/securekeys/" + KEY2, GSON.toJson(secureKeyCreateRequest));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet("/v3/namespaces/default/securekeys/");
    String result2 = readResponse(response);
    list = GSON.fromJson(result2, listType);
    expected.add(new SecureKeyListEntry(KEY2, DESCRIPTION2));
    for (SecureKeyListEntry entry : list) {
      Assert.assertTrue(expected.contains(entry));
    }

    // After deleting an element
    delete(KEY);
    response = doGet("/v3/namespaces/default/securekeys/");
    String result3 = readResponse(response);
    list = GSON.fromJson(result3, listType);
    expected.remove(new SecureKeyListEntry(KEY, DESCRIPTION));
    for (SecureKeyListEntry entry : list) {
      Assert.assertTrue(expected.contains(entry));
    }
  }

  public HttpResponse delete(String key) throws Exception {
    return doDelete("/v3/namespaces/default/securekeys/" + key);
  }
}
