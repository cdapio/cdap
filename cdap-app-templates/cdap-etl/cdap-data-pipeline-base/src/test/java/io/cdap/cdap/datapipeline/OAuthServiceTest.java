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
    Assert.assertEquals("http://www.example.com/login?client_id=clientid&redirect_uri=null", authURL);
  }

  @Test
  public void testCreateProviderWithClientCredentialsMissing() throws IOException {
    // Attempt to create provider with missing client credentials should fail with 400 status code.
    String loginURL = "http://www.example.com/login";
    String tokenRefreshURL = "http://www.example.com/token";
    PutOAuthProviderRequest request = new PutOAuthProviderRequest(loginURL, tokenRefreshURL, null, null);
    HttpResponse createResponse = makePutCall("provider/testprovider", request);
    Assert.assertEquals(400, createResponse.getResponseCode());
  }

  @Test
  public void testCreateProviderWithReuseClientCredentialsTrue() throws IOException {
    // Attempt to create provider with no client credentials and 'reuse_client_credentials' query
    // param 'true' should succeed with 200 status code.
    String loginURL = "http://www.example.com/login";
    String tokenRefreshURL = "http://www.example.com/token";
    PutOAuthProviderRequest request = new PutOAuthProviderRequest(loginURL, tokenRefreshURL, null, null);
    HttpResponse createResponse = makePutCall("provider/testprovider10?reuse_client_credentials=true", request);
    Assert.assertEquals(500, createResponse.getResponseCode());
  }

  @Test
  public void testCreateProviderReuseCredentialsWithReuseClientCredentialsTrue() throws IOException {
    // Attempt to create provider with client credentials.
    String loginURL = "http://www.example.com/login20";
    String tokenRefreshURL = "http://www.example.com/token20";
    String clientId = "clientid";
    String clientSecret = "clientsecret";
    PutOAuthProviderRequest request = new PutOAuthProviderRequest(loginURL, tokenRefreshURL, clientId, clientSecret);
    HttpResponse createResponse = makePutCall("provider/testprovider20", request);
    Assert.assertEquals(200, createResponse.getResponseCode());

    // Attempt to update provider with no client credentials and 'reuse_client_credentials' query
    // param 'true' should succeed with 200 status code.
    loginURL = "http://www.example.com/login21";
    tokenRefreshURL = "http://www.example.com/token21";
    request = new PutOAuthProviderRequest(loginURL, tokenRefreshURL, null, null);
    createResponse = makePutCall("provider/testprovider20?reuse_client_credentials=true", request);
    Assert.assertEquals(200, createResponse.getResponseCode());
  }

  @Test
  public void testCreateProviderWithReuseClientCredentialsFalse() throws IOException {
    // Attempt to create provider with missing client credentials and 'reuse_client_credentials'
    // query param 'false' should fail with 400 status code.
    String loginURL = "http://www.example.com/login30";
    String tokenRefreshURL = "http://www.example.com/token30";
    PutOAuthProviderRequest request = new PutOAuthProviderRequest(loginURL, tokenRefreshURL, null, null);
    HttpResponse createResponse = makePutCall("provider/testprovider30?reuse_client_credentials=false", request);
    Assert.assertEquals(400, createResponse.getResponseCode());
  }

  @Test
  public void testGetAuthURLForMissingClientCredentials() throws IOException {
    // Attempt to create provider with missing client credentials and 'reuse_client_credentials'
    // query param 'true'.
    String loginURL = "http://www.example.com/login40";
    String tokenRefreshURL = "http://www.example.com/token40";
    PutOAuthProviderRequest request = new PutOAuthProviderRequest(loginURL, tokenRefreshURL, null, null);
    HttpResponse createResponse = makePutCall("provider/testprovider40?reuse_client_credentials=false", request);
    Assert.assertEquals(400, createResponse.getResponseCode());

    // Get OAuth login URL should fail with 404 as client credentials are not configured.
    HttpResponse getResponse = makeGetCall("provider/testprovider40/authurl");
    Assert.assertEquals(404, getResponse.getResponseCode());
  }

  @Test
  public void testGetAuthURLForReusedClientCredentials() throws IOException {
    // Attempt to create provider with client credentials.
    String loginURL = "http://www.example.com/login50";
    String tokenRefreshURL = "http://www.example.com/token50";
    String clientId = "clientid";
    String clientSecret = "clientsecret";
    PutOAuthProviderRequest request = new PutOAuthProviderRequest(loginURL, tokenRefreshURL, clientId, clientSecret);
    HttpResponse createResponse = makePutCall("provider/testprovider50", request);
    Assert.assertEquals(200, createResponse.getResponseCode());

    // Grab OAuth login URL to verify write succeeded.
    HttpResponse getResponse = makeGetCall("provider/testprovider50/authurl");
    Assert.assertEquals(200, getResponse.getResponseCode());
    String authURL = getResponse.getResponseBodyAsString();
    Assert.assertEquals("http://www.example.com/login50?client_id=clientid&redirect_uri=null", authURL);

    // Attempt to update provider with with missing credentials and 'reuse_client_credentials' query
    // param 'true' should succeed with 200 status code.
    loginURL = "http://www.example.com/login51";
    tokenRefreshURL = "http://www.example.com/token51";
    request = new PutOAuthProviderRequest(loginURL, tokenRefreshURL, null, null);
    createResponse = makePutCall("provider/testprovider50?reuse_client_credentials=true", request);
    Assert.assertEquals(200, createResponse.getResponseCode());

    // Grab OAuth login URL to verify write succeeded.
    getResponse = makeGetCall("provider/testprovider50/authurl");
    Assert.assertEquals(200, getResponse.getResponseCode());
    authURL = getResponse.getResponseBodyAsString();
    Assert.assertEquals("http://www.example.com/login51?client_id=clientid&redirect_uri=null", authURL);
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
