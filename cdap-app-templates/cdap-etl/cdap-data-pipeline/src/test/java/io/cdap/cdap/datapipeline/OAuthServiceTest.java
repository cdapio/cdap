/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.datapipeline;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.datapipeline.oauth.PutOAuthProviderRequest;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

public class OAuthServiceTest extends DataPipelineServiceTest {

  private static final Gson GSON = new GsonBuilder()
      .setPrettyPrinting()
      .create();

  @Test
  public void testCreateProvider() throws IOException {
    // Attempt to create provider
    String loginURL = "http://www.example.com/login";
    String tokenRefreshURL = "http://www.example.com/token";
    String clientId = "clientid";
    String clientSecret = "clientsecret";
    PutOAuthProviderRequest request = new PutOAuthProviderRequest(loginURL, tokenRefreshURL, clientId, clientSecret);
    HttpResponse createResponse = makePutCall("provider/testprovider", request);
    Assert.assertEquals(200, createResponse.getResponseCode());

    // Grab OAuth login URL to verify write succeeded
    HttpResponse getResponse = makeGetCall("provider/testprovider/authurl");
    Assert.assertEquals(200, getResponse.getResponseCode());
    String authURL = getResponse.getResponseBodyAsString();
    Assert.assertEquals("http://www.example.com/login?response_type=code"
        + "&client_id=clientid&redirect_uri=null&scope=refresh_token%20api", authURL);
  }

  @Test
  public void testCreateProviderBadLoginURL() throws IOException {
    // Attempt to create provider
    String loginURL = "badurl";
    String tokenRefreshURL = "http://www.example.com/token";
    String clientId = "clientid";
    String clientSecret = "clientsecret";
    PutOAuthProviderRequest request = new PutOAuthProviderRequest(loginURL, tokenRefreshURL, clientId, clientSecret);
    HttpResponse createResponse = makePutCall("provider/testprovider", request);
    Assert.assertEquals(400, createResponse.getResponseCode());
  }

  @Test
  public void testCreateProviderBadTokenRefreshURL() throws IOException {
    // Attempt to create provider
    String loginURL = "http://www.example.com/token";
    String tokenRefreshURL = "badurl";
    String clientId = "clientid";
    String clientSecret = "clientsecret";
    PutOAuthProviderRequest request = new PutOAuthProviderRequest(loginURL, tokenRefreshURL, clientId, clientSecret);
    HttpResponse createResponse = makePutCall("provider/testprovider", request);
    Assert.assertEquals(400, createResponse.getResponseCode());
  }

  @Test
  public void testGetAuthURLProviderDoesNotExist() throws IOException {
    HttpResponse getResponse = makeGetCall("provider/nonexistantprovider/authurl");
    Assert.assertEquals(404, getResponse.getResponseCode());
  }

  private HttpResponse makeGetCall(String endpoint) throws IOException {
    URL url = serviceURI
        .resolve(String.format("v1/oauth/%s", endpoint))
        .toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.GET, url).build();
    return HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
  }

  private <T> T makeGetCall(String endpoint, Class<T> clazz) throws IOException {
    HttpResponse response = makeGetCall(endpoint);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), clazz);
  }

  private <T> HttpResponse makePutCall(String endpoint, T body) throws IOException {
    URL url = serviceURI
        .resolve(String.format("v1/oauth/%s", endpoint))
        .toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.PUT, url)
        .withBody(GSON.toJson(body))
        .build();
    return HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
  }

}
