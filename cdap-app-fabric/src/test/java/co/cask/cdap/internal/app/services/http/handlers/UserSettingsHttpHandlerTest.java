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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.gateway.handlers.UserSettingsHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
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
    HttpResponse response = doGet("/v3/configuration/usersettings");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
    response = doGet("/v3/configuration/usersettings/properties");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = GSON.fromJson(s, MAP_STRING_STRING_TYPE);
    Assert.assertEquals(0, o.size());

    response = doPut("/v3/configuration/usersettings/properties", "{'name':'abcd', 'age':'23'}");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet("/v3/configuration/usersettings/properties");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = GSON.fromJson(s, MAP_STRING_STRING_TYPE);
    Assert.assertEquals(2, o.size());
    Assert.assertEquals("abcd", o.get("name"));
    Assert.assertEquals("23", o.get("age"));

    response = doPut("/v3/configuration/usersettings/properties", "{'age':'300', 'univ':'Stanford'}");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet("/v3/configuration/usersettings/properties");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = GSON.fromJson(s, MAP_STRING_STRING_TYPE);
    Assert.assertEquals(2, o.size());
    Assert.assertEquals("300", o.get("age"));
    Assert.assertEquals("Stanford", o.get("univ"));

    response = doDelete("/v3/configuration/usersettings/properties");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet("/v3/configuration/usersettings/properties");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = GSON.fromJson(s, MAP_STRING_STRING_TYPE);
    Assert.assertEquals(0, o.size());
  }
}
