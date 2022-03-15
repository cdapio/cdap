/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.RestartServiceInstancesStatus;
import io.cdap.cdap.proto.SystemServiceMeta;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;

/**
 * Monitor handler tests.
 */
public class MonitorHandlerTest extends AppFabricTestBase {

  private HttpURLConnection openURL(String path, HttpMethod method) throws IOException {
    HttpURLConnection urlConn = (HttpURLConnection) createURL(path).openConnection();
    if (urlConn instanceof HttpsURLConnection) {
      new HttpsEnabler().setTrustAll(true).enable((HttpsURLConnection) urlConn);
    }
    urlConn.setRequestMethod(method.name());
    return urlConn;
  }

  private URL createURL(String path) throws MalformedURLException {
    return getEndPoint(String.format("/v3/%s", path)).toURL();
  }

  @Test
  public void testSystemServices() throws Exception {

    Type token = new TypeToken<List<SystemServiceMeta>>() { }.getType();

    HttpURLConnection urlConn = openURL("system/services", HttpMethod.GET);
    Assert.assertEquals(HttpResponseStatus.OK.code(), urlConn.getResponseCode());
    List<SystemServiceMeta> actual = GSON.fromJson(new String(ByteStreams.toByteArray(urlConn.getInputStream()),
                                                       Charsets.UTF_8), token);

    Assert.assertEquals(9, actual.size());
    urlConn.disconnect();
  }


  @Test
  public void testSystemServicesStatus() throws Exception {

    Type token = new TypeToken<Map<String, String>>() { }.getType();

    HttpURLConnection urlConn = openURL("system/services/status", HttpMethod.GET);
    Assert.assertEquals(HttpResponseStatus.OK.code(), urlConn.getResponseCode());

    Map<String, String> result = GSON.fromJson(new String(ByteStreams.toByteArray(urlConn.getInputStream()),
                                               Charsets.UTF_8), token);
    Assert.assertEquals(9, result.size());
    urlConn.disconnect();
    Assert.assertEquals("OK", result.get(Constants.Service.APP_FABRIC_HTTP));
  }

  @Test
  public void testInstances() throws Exception {

    Type token = new TypeToken<Map<String, Integer>>() { }.getType();

    String path = String.format("system/services/%s/instances", Constants.Service.APP_FABRIC_HTTP);
    HttpURLConnection urlConn = openURL(path, HttpMethod.GET);
    Assert.assertEquals(HttpResponseStatus.OK.code(), urlConn.getResponseCode());

    Map<String, Integer> result = GSON.fromJson(new String(ByteStreams.toByteArray(urlConn.getInputStream()),
                                                          Charsets.UTF_8), token);
    Assert.assertEquals(2, result.size());
    urlConn.disconnect();
    Assert.assertEquals(1, (int) result.get("requested"));
    Assert.assertEquals(1, (int) result.get("provisioned"));
  }

  @Test
  public void testRestartInstances() throws Exception {
    String path = String.format("%s/system/services/%s/restart", Constants.Gateway.API_VERSION_3,
                                Constants.Service.APP_FABRIC_HTTP);
    HttpResponse response = doPost(path);

    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    path = String.format("%s/system/services/%s/latest-restart", Constants.Gateway.API_VERSION_3,
                         Constants.Service.APP_FABRIC_HTTP);
    response = doGet(path);

    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    RestartServiceInstancesStatus result = GSON.fromJson(response.getResponseBodyAsString(),
                                                         RestartServiceInstancesStatus.class);

    Assert.assertNotNull(result);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result.getServiceName());
    Assert.assertEquals(RestartServiceInstancesStatus.RestartStatus.SUCCESS, result.getStatus());
  }

  @Test
  public void testSingleIdRestartInstances() throws Exception {
    String path = String.format("%s/system/services/%s/instances/0/restart", Constants.Gateway.API_VERSION_3,
                                Constants.Service.APP_FABRIC_HTTP);
    HttpResponse response = doPost(path);

    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    path = String.format("%s/system/services/%s/latest-restart", Constants.Gateway.API_VERSION_3,
                         Constants.Service.APP_FABRIC_HTTP);
    response = doGet(path);

    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    RestartServiceInstancesStatus result = GSON.fromJson(response.getResponseBodyAsString(),
                                                         RestartServiceInstancesStatus.class);

    Assert.assertNotNull(result);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result.getServiceName());
    Assert.assertEquals(RestartServiceInstancesStatus.RestartStatus.SUCCESS, result.getStatus());
  }

  @Test
  public void testInvalidIdRestartInstances() throws Exception {
    String path = String.format("%s/system/services/%s/instances/1000/restart", Constants.Gateway.API_VERSION_3,
                                Constants.Service.APP_FABRIC_HTTP);
    HttpResponse response = doPost(path);

    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.getResponseCode());

    path = String.format("%s/system/services/%s/latest-restart", Constants.Gateway.API_VERSION_3,
                         Constants.Service.APP_FABRIC_HTTP);
    response = doGet(path);

    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    RestartServiceInstancesStatus result = GSON.fromJson(response.getResponseBodyAsString(),
                                                         RestartServiceInstancesStatus.class);

    Assert.assertNotNull(result);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result.getServiceName());
    Assert.assertEquals(RestartServiceInstancesStatus.RestartStatus.FAILURE, result.getStatus());
  }
}
