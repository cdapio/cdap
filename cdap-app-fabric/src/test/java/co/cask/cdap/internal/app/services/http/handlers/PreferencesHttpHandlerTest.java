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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.WordCountApp;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.PreferencesHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Tests for {@link PreferencesHttpHandler}
 */
public class PreferencesHttpHandlerTest extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final String NAMESPACE1 = "testnamespace1";
  private static final String NAMESPACE2 = "testnamespace2";
  private static final NamespaceMeta NAMESPACE1_META = new NamespaceMeta.Builder()
    .setName(NAMESPACE1).setDescription(NAMESPACE1).build();
  private static final NamespaceMeta NAMESPACE2_META = new NamespaceMeta.Builder()
    .setName(NAMESPACE2).setDescription(NAMESPACE2).build();

  @BeforeClass
  public static void setup() throws Exception {
    HttpResponse response = doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, NAMESPACE1),
                                  GSON.toJson(NAMESPACE1_META));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, NAMESPACE2),
                     GSON.toJson(NAMESPACE2_META));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testInstance() throws Exception {
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getProperty(getURI(), false, 200));
    Assert.assertEquals(propMap, getProperty(getURI(), true, 200));
    propMap.put("k1", "3@#3");
    propMap.put("@#$#ljfds", "231@#$");
    setProperty(getURI(), propMap, 200);
    Assert.assertEquals(propMap, getProperty(getURI(), false, 200));
    Assert.assertEquals(propMap, getProperty(getURI(), true, 200));
    propMap.clear();
    deleteProperty(getURI(), 200);
    Assert.assertEquals(propMap, getProperty(getURI(), false, 200));
    Assert.assertEquals(propMap, getProperty(getURI(), true, 200));
  }

  @Test
  public void testNamespace() throws Exception {
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getProperty(getURI(NAMESPACE1), false, 200));
    Assert.assertEquals(propMap, getProperty(getURI(NAMESPACE2), false, 200));
    Assert.assertEquals(propMap, getProperty(getURI(NAMESPACE1), true, 200));
    Assert.assertEquals(propMap, getProperty(getURI(NAMESPACE2), true, 200));
    propMap.put("k1", "3@#3");
    propMap.put("@#$#ljfds", "231@#$");
    setProperty(getURI(NAMESPACE1), propMap, 200);
    Assert.assertEquals(propMap, getProperty(getURI(NAMESPACE1), false, 200));
    Assert.assertEquals(propMap, getProperty(getURI(NAMESPACE1), true, 200));

    Map<String, String> instanceMap = Maps.newHashMap();
    instanceMap.put("k1", "432432*#######");
    setProperty(getURI(), instanceMap, 200);
    Assert.assertEquals(instanceMap, getProperty(getURI(), true, 200));
    Assert.assertEquals(instanceMap, getProperty(getURI(NAMESPACE2), true, 200));
    Assert.assertEquals(propMap, getProperty(getURI(NAMESPACE1), true, 200));

    instanceMap.put("k2", "(93424");
    setProperty(getURI(), instanceMap, 200);
    instanceMap.putAll(propMap);
    Assert.assertEquals(instanceMap, getProperty(getURI(NAMESPACE1), true, 200));

    deleteProperty(getURI(NAMESPACE1), 200);
    deleteProperty(getURI(NAMESPACE2), 200);

    instanceMap.clear();
    instanceMap.put("*&$kjh", "*(&*1");
    setProperty(getURI(), instanceMap, 200);
    Assert.assertEquals(instanceMap, getProperty(getURI(NAMESPACE2), true, 200));
    Assert.assertEquals(instanceMap, getProperty(getURI(NAMESPACE1), true, 200));
    instanceMap.clear();
    Assert.assertEquals(instanceMap, getProperty(getURI(NAMESPACE2), false, 200));
    Assert.assertEquals(instanceMap, getProperty(getURI(NAMESPACE1), false, 200));

    deleteProperty(getURI(), 200);
    Assert.assertEquals(instanceMap, getProperty(getURI(NAMESPACE2), true, 200));
    Assert.assertEquals(instanceMap, getProperty(getURI(NAMESPACE1), true, 200));
    getProperty(getURI("invalidNamespace"), true, 404);
  }

  @Test
  public void testApplication() throws Exception {
    deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, NAMESPACE1);
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getProperty(getURI(NAMESPACE1, "WordCountApp"), false, 200));
    Assert.assertEquals(propMap, getProperty(getURI(NAMESPACE1, "WordCountApp"), true, 200));
    getProperty(getURI(NAMESPACE1, "InvalidAppName"), false, 404);
    setProperty(getURI(), ImmutableMap.of("k1", "instance"), 200);
    setProperty(getURI(NAMESPACE1), ImmutableMap.of("k1", "namespace"), 200);
    setProperty(getURI(NAMESPACE1, "WordCountApp"), ImmutableMap.of("k1", "application"), 200);
    Assert.assertEquals("application", getProperty(getURI(NAMESPACE1, "WordCountApp"), false, 200).get("k1"));
    Assert.assertEquals("application", getProperty(getURI(NAMESPACE1, "WordCountApp"), true, 200).get("k1"));
    Assert.assertEquals("namespace", getProperty(getURI(NAMESPACE1), false, 200).get("k1"));
    Assert.assertEquals("namespace", getProperty(getURI(NAMESPACE1), true, 200).get("k1"));
    Assert.assertEquals("instance", getProperty(getURI(), true, 200).get("k1"));
    Assert.assertEquals("instance", getProperty(getURI(), false, 200).get("k1"));
    deleteProperty(getURI(NAMESPACE1, "WordCountApp"), 200);
    Assert.assertEquals("namespace", getProperty(getURI(NAMESPACE1, "WordCountApp"), true, 200).get("k1"));
    Assert.assertNull(getProperty(getURI(NAMESPACE1, "WordCountApp"), false, 200).get("k1"));
    deleteProperty(getURI(NAMESPACE1), 200);
    Assert.assertEquals("instance", getProperty(getURI(NAMESPACE1, "WordCountApp"), true, 200).get("k1"));
    Assert.assertEquals("instance", getProperty(getURI(NAMESPACE1), true, 200).get("k1"));
    Assert.assertNull(getProperty(getURI(NAMESPACE1), false, 200).get("k1"));
    deleteProperty(getURI(), 200);
    Assert.assertNull(getProperty(getURI(), true, 200).get("k1"));
    Assert.assertNull(getProperty(getURI(NAMESPACE1), true, 200).get("k1"));
    Assert.assertNull(getProperty(getURI(NAMESPACE1, "WordCountApp"), true, 200).get("k1"));
  }

  @Test
  public void testProgram() throws Exception {
    deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, NAMESPACE2);
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getProperty(getURI(NAMESPACE2, "WordCountApp", "flows", "WordCountFlow"), false, 200));
    getProperty(getURI(NAMESPACE2, "WordCountApp", "invalidType", "somename"), false, 400);
    getProperty(getURI(NAMESPACE2, "WordCountApp", "flows", "somename"), false, 404);
    propMap.put("k1", "k349*&#$");
    setProperty(getURI(NAMESPACE2, "WordCountApp", "flows", "WordCountFlow"), propMap, 200);
    Assert.assertEquals(propMap, getProperty(getURI(NAMESPACE2, "WordCountApp", "flows", "WordCountFlow"), false, 200));
    propMap.put("k1", "instance");
    setProperty(getURI(), propMap, 200);
    Assert.assertEquals(propMap, getProperty(getURI(), true, 200));
    propMap.put("k1", "k349*&#$");
    Assert.assertEquals(propMap, getProperty(getURI(NAMESPACE2, "WordCountApp", "flows", "WordCountFlow"), false, 200));
    deleteProperty(getURI(NAMESPACE2, "WordCountApp", "flows", "WordCountFlow"), 200);
    propMap.put("k1", "instance");
    Assert.assertEquals(0, getProperty(getURI(NAMESPACE2, "WordCountApp", "flows", "WordCountFlow"),
                                       false, 200).size());
    Assert.assertEquals(propMap, getProperty(getURI(NAMESPACE2, "WordCountApp", "flows", "WordCountFlow"), true, 200));
    deleteProperty(getURI(), 200);
    propMap.clear();
    Assert.assertEquals(propMap, getProperty(getURI(NAMESPACE2, "WordCountApp", "flows", "WordCountFlow"), false, 200));
    Assert.assertEquals(propMap, getProperty(getURI(), false, 200));
  }

  private String getURI() {
    return "";
  }

  private String getURI(String namespace) {
    return String.format("%s/namespaces/%s", getURI(), namespace);
  }

  private String getURI(String namespace, String appId) {
    return String.format("%s/apps/%s", getURI(namespace), appId);
  }

  private String getURI(String namespace, String appId, String programType, String programId) {
    return String.format("%s/%s/%s", getURI(namespace, appId), programType, programId);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    HttpResponse response = doDelete(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, NAMESPACE2));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  private void setProperty(String uri, Map<String, String> props, int expectedStatus) throws Exception {
    HttpResponse response = doPut(String.format("/v3/configuration/preferences/%s", uri), GSON.toJson(props));
    Assert.assertEquals(expectedStatus, response.getStatusLine().getStatusCode());
  }

  private Map<String, String> getProperty(String uri, boolean resolved, int expectedStatus) throws Exception {
    String request = String.format("/v3/configuration/preferences/%s", uri);
    if (resolved) {
      request += "?resolved=true";
    }
    HttpResponse response = doGet(request);
    Assert.assertEquals(expectedStatus, response.getStatusLine().getStatusCode());
    if (expectedStatus == 200) {
      String s = EntityUtils.toString(response.getEntity());
      return GSON.fromJson(s, MAP_STRING_STRING_TYPE);
    }
    return null;
  }

  private void deleteProperty(String uri, int expectedStatus) throws Exception {
    HttpResponse response = doDelete(String.format("/v3/configuration/preferences/%s", uri));
    Assert.assertEquals(expectedStatus, response.getStatusLine().getStatusCode());
  }
}
