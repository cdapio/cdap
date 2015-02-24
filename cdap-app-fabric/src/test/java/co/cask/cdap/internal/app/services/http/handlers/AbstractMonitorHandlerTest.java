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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.SystemServiceMeta;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Abstract monitor handler tests.
 */
public abstract class AbstractMonitorHandlerTest extends AppFabricTestBase {

  protected HttpURLConnection openURL(URL url, HttpMethod method) throws IOException {
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setRequestMethod(method.getName());
    return urlConn;
  }

  protected abstract URL createURL(String path) throws URISyntaxException, MalformedURLException;

  @Test
  public void testSystemServices() throws Exception {

    Type token = new TypeToken<List<SystemServiceMeta>>() { }.getType();

    HttpURLConnection urlConn = openURL(createURL("system/services"), HttpMethod.GET);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    List<SystemServiceMeta> actual = GSON.fromJson(new String(ByteStreams.toByteArray(urlConn.getInputStream()),
                                                       Charsets.UTF_8), token);

    Assert.assertEquals(7, actual.size());
    urlConn.disconnect();
  }


  @Test
  public void testSystemServicesStatus() throws Exception {

    Type token = new TypeToken<Map<String, String>>() { }.getType();

    HttpURLConnection urlConn = openURL(createURL("system/services/status"), HttpMethod.GET);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());

    Map<String, String> result = GSON.fromJson(new String(ByteStreams.toByteArray(urlConn.getInputStream()),
                                               Charsets.UTF_8), token);
    Assert.assertEquals(7, result.size());
    urlConn.disconnect();
    Assert.assertEquals("OK", result.get(Constants.Service.APP_FABRIC_HTTP));
  }

  @Test
  public void testInstances() throws Exception {

    Type token = new TypeToken<Map<String, Integer>>() { }.getType();

    String path = String.format("system/services/%s/instances", Constants.Service.APP_FABRIC_HTTP);
    HttpURLConnection urlConn = openURL(createURL(path), HttpMethod.GET);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());

    Map<String, Integer> result = GSON.fromJson(new String(ByteStreams.toByteArray(urlConn.getInputStream()),
                                                          Charsets.UTF_8), token);
    Assert.assertEquals(2, result.size());
    urlConn.disconnect();
    Assert.assertEquals(1, (int) result.get("requested"));
    Assert.assertEquals(1, (int) result.get("provisioned"));
  }
}
