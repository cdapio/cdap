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

import co.cask.cdap.gateway.handlers.UserSettingsHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Tests for {@link UserSettingsHttpHandler}
 */
public class UserSettingsHttpHandlerTest extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  @Test
  public void testSettings() throws Exception {
    //Check if settings are empty
    Map<String, String> propMap = Maps.newHashMap();
    JsonObject emptyElement = getProperty(200).getAsJsonObject();
    Assert.assertEquals(2, emptyElement.entrySet().size());
    Assert.assertEquals(propMap, GSON.fromJson(emptyElement.get("property"), MAP_STRING_STRING_TYPE));

    //Put some settings and verify if its persisted
    propMap.put("k1", "43242!@#!@#");
    propMap.put("@##@!#", "v2131231!@#!");
    putProperty(propMap, 200);
    emptyElement = getProperty(200).getAsJsonObject();
    Assert.assertEquals(2, emptyElement.entrySet().size());
    Assert.assertEquals(propMap, GSON.fromJson(emptyElement.get("property"), MAP_STRING_STRING_TYPE));

    //Delete the settings and verify that its empty
    propMap.clear();
    deleteProperty(200);
    emptyElement = getProperty(200).getAsJsonObject();
    Assert.assertEquals(2, emptyElement.entrySet().size());
    Assert.assertEquals(propMap, GSON.fromJson(emptyElement.get("property"), MAP_STRING_STRING_TYPE));
  }

  private void putProperty(Map<String, String> props, int expectedStatus) throws Exception {
    HttpResponse response = doPut("/v3/configuration/usersettings", GSON.toJson(props));
    Assert.assertEquals(expectedStatus, response.getStatusLine().getStatusCode());
  }

  private JsonElement getProperty(int expectedStatus) throws Exception {
    HttpResponse response = doGet("/v3/configuration/usersettings");
    Assert.assertEquals(expectedStatus, response.getStatusLine().getStatusCode());
    if (expectedStatus == HttpResponseStatus.OK.code()) {
      String jsonData = EntityUtils.toString(response.getEntity());
      return new JsonParser().parse(jsonData);
    }
    return null;
  }

  private void deleteProperty(int expectedStatus) throws Exception {
    HttpResponse response = doDelete("/v3/configuration/usersettings");
    Assert.assertEquals(expectedStatus, response.getStatusLine().getStatusCode());
  }
}
