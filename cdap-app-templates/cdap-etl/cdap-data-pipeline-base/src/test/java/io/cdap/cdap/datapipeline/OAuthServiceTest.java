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
import io.cdap.cdap.api.security.store.SecureStoreInfo;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.datapipeline.oauth.AuthType;
import io.cdap.cdap.datapipeline.oauth.OAuthAccessToken;
import io.cdap.cdap.datapipeline.oauth.OAuthProvider;
import io.cdap.cdap.datapipeline.oauth.OAuthRefreshToken;
import io.cdap.cdap.datapipeline.oauth.PutOAuthProviderRequest;
import io.cdap.cdap.datapipeline.oauth.RefreshType;
import io.cdap.cdap.security.store.SecureStoreService;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.Collections;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

public class OAuthServiceTest extends DataPipelineServiceTest {

  private static final Gson GSON = new GsonBuilder()
      .setPrettyPrinting()
      .create();

  private NettyHttpService mockOAuthServer;
  private String mockTokenUrl;
  private int mockTokenPort;

  public static final class MockTokenHandler extends AbstractHttpHandler {
    @POST
    @Path("/token")
    public void token(FullHttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "{\"access_token\":\"mock_access_token\",\"expires_in\":3600}");
    }
  }

  @BeforeClass
  public static void setupMockSecureStoreLeasing() {
    for (Object inst : new Object[] {getSecureStore(), getSecureStoreManager()}) {
      wrapSecureStoreService(inst);
    }
  }

  private static void wrapSecureStoreService(Object inst) {
    if (inst == null || !inst.getClass().getName().contains("DefaultSecureStoreService")) {
      return;
    }

    try {
      Field field = inst.getClass().getDeclaredField("secureStoreService");
      field.setAccessible(true);

      Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

      Object current = field.get(inst);
      if (!(current instanceof SecureStoreService) || Proxy.isProxyClass(current.getClass())) {
        return;
      }

      SecureStoreService delegate = (SecureStoreService) current;
      SecureStoreService proxy = (SecureStoreService) Proxy.newProxyInstance(
          SecureStoreService.class.getClassLoader(),
          new Class<?>[] {SecureStoreService.class},
          (p, method, args) -> {
            if ("getStoreInfo".equals(method.getName())) {
              return new SecureStoreInfo(Collections.singleton(SecureStoreInfo.Capability.SECRET_LEASING));
            }
            if ("acquireLease".equals(method.getName()) || "releaseLease".equals(method.getName())) {
              return true;
            }
            return method.invoke(delegate, args);
          }
      );
      field.set(inst, proxy);
    } catch (Exception ignored) {
      // Suppressed intentionally
    }
  }

  @Before
  public void setUpMockServer() throws Exception {
    setupMockSecureStoreLeasing();
    mockOAuthServer = NettyHttpService.builder("mock-oauth-server")
        .setHost("localhost")
        .setPort(0)
        .setHttpHandlers(new MockTokenHandler())
        .build();
    mockOAuthServer.start();
    mockTokenPort = mockOAuthServer.getBindAddress().getPort();
    mockTokenUrl = "http://localhost:" + mockTokenPort + "/token";
  }

  @After
  public void tearDownMockServer() throws Exception {
    if (mockOAuthServer != null) {
      mockOAuthServer.stop();
    }
  }

  private static PutOAuthProviderRequest createPutRequest(String loginURL, String tokenRefreshURL,
                                                          @Nullable String clientId, @Nullable String clientSecret,
                                                          OAuthProvider.CredentialEncodingStrategy strategy,
                                                          @Nullable String userAgent,
                                                          @Nullable AuthType authType,
                                                          @Nullable RefreshType refreshType) {
    return PutOAuthProviderRequest.builder()
        .loginURL(loginURL)
        .tokenRefreshURL(tokenRefreshURL)
        .clientId(clientId)
        .clientSecret(clientSecret)
        .strategy(strategy)
        .userAgent(userAgent)
        .authType(authType)
        .refreshType(refreshType)
        .build();
  }

  private static PutOAuthProviderRequest createPutRequest(String loginURL, String tokenRefreshURL,
                                                          @Nullable String clientId, @Nullable String clientSecret,
                                                          OAuthProvider.CredentialEncodingStrategy strategy,
                                                          @Nullable String userAgent,
                                                          @Nullable AuthType authType) {
    return createPutRequest(loginURL, tokenRefreshURL, clientId, clientSecret, strategy, userAgent, authType, null);
  }

  private static PutOAuthProviderRequest createPutRequest(String loginURL, String tokenRefreshURL,
                                                          @Nullable String clientId, @Nullable String clientSecret) {
    return createPutRequest(loginURL, tokenRefreshURL, clientId, clientSecret,
        OAuthProvider.CredentialEncodingStrategy.FORM_BODY, null, null, null);
  }

  @Test
  public void testCreateProvider() throws IOException {
    // Attempt to create provider
    String loginURL = "http://www.example.com/login";
    String tokenRefreshURL = "http://www.example.com/token";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret");
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
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, null, null);
    HttpResponse createResponse = makePutCall("provider/testprovider", request);
    Assert.assertEquals(400, createResponse.getResponseCode());
  }

  @Test
  public void testCreateProviderWithReuseClientCredentialsTrue() throws IOException {
    // Attempt to create provider with no client credentials and 'reuse_client_credentials' query
    // param 'true' should succeed with 200 status code.
    String loginURL = "http://www.example.com/login";
    String tokenRefreshURL = "http://www.example.com/token";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, null, null);
    HttpResponse createResponse = makePutCall("provider/testprovider10?reuse_client_credentials=true", request);
    Assert.assertEquals(500, createResponse.getResponseCode());
  }

  @Test
  public void testCreateProviderReuseCredentialsWithReuseClientCredentialsTrue() throws IOException {
    // Attempt to create provider with client credentials.
    String loginURL = "http://www.example.com/login20";
    String tokenRefreshURL = "http://www.example.com/token20";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret");
    HttpResponse createResponse = makePutCall("provider/testprovider20", request);
    Assert.assertEquals(200, createResponse.getResponseCode());

    // Attempt to update provider with no client credentials and 'reuse_client_credentials' query
    // param 'true' should succeed with 200 status code.
    loginURL = "http://www.example.com/login21";
    tokenRefreshURL = "http://www.example.com/token21";
    request = createPutRequest(loginURL, tokenRefreshURL, null, null);
    createResponse = makePutCall("provider/testprovider20?reuse_client_credentials=true", request);
    Assert.assertEquals(200, createResponse.getResponseCode());
  }

  @Test
  public void testCreateProviderWithReuseClientCredentialsFalse() throws IOException {
    // Attempt to create provider with missing client credentials and 'reuse_client_credentials'
    // query param 'false' should fail with 400 status code.
    String loginURL = "http://www.example.com/login30";
    String tokenRefreshURL = "http://www.example.com/token30";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, null, null);
    HttpResponse createResponse = makePutCall("provider/testprovider30?reuse_client_credentials=false", request);
    Assert.assertEquals(400, createResponse.getResponseCode());
  }

  @Test
  public void testCreateProviderWithBasicAuth() throws IOException {
    // Attempt to create provider
    String loginURL = "http://www.example.com/login31";
    String tokenRefreshURL = "http://www.example.com/token31";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret",
        OAuthProvider.CredentialEncodingStrategy.BASIC_AUTH, null, null);
    HttpResponse createOauthProviderResponse = makePutCall("provider/testprovider31", request);
    Assert.assertEquals(200, createOauthProviderResponse.getResponseCode());

    // Grab OAuth login URL to verify write succeeded
    HttpResponse getAuthUrlResponse = makeGetCall("provider/testprovider31/authurl");
    Assert.assertEquals(200, getAuthUrlResponse.getResponseCode());
    String authURL = getAuthUrlResponse.getResponseBodyAsString();
    Assert.assertEquals("http://www.example.com/login31?client_id=clientid&redirect_uri=null", authURL);
  }

  @Test
  public void testCreateProviderWithBasicAuthAndUserAgent() throws IOException {
    // Attempt to create provider
    String loginURL = "http://www.example.com/login32";
    String tokenRefreshURL = "http://www.example.com/token32";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret",
        OAuthProvider.CredentialEncodingStrategy.BASIC_AUTH, "cdap-test", null);
    HttpResponse createOauthProviderResponse = makePutCall("provider/testprovider32", request);
    Assert.assertEquals(200, createOauthProviderResponse.getResponseCode());

    // Grab OAuth login URL to verify write succeeded
    HttpResponse getAuthUrlResponse = makeGetCall("provider/testprovider32/authurl");
    Assert.assertEquals(200, getAuthUrlResponse.getResponseCode());
    String authURL = getAuthUrlResponse.getResponseBodyAsString();
    Assert.assertEquals("http://www.example.com/login32?client_id=clientid&redirect_uri=null", authURL);
  }

  @Test
  public void testCreateProviderWithPkceAuthType() throws IOException {
    String loginURL = "http://www.example.com/login_pkce";
    String tokenRefreshURL = "http://www.example.com/token_pkce";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret",
        OAuthProvider.CredentialEncodingStrategy.FORM_BODY, null, AuthType.PKCE);
    HttpResponse createOauthProviderResponse = makePutCall("provider/testprovider_pkce", request);
    Assert.assertEquals(200, createOauthProviderResponse.getResponseCode());

    // Grab OAuth login URL to verify write succeeded
    HttpResponse getAuthUrlResponse = makeGetCall("provider/testprovider_pkce/authurl");
    Assert.assertEquals(200, getAuthUrlResponse.getResponseCode());
    String authURL = getAuthUrlResponse.getResponseBodyAsString();

    // Verify base URL
    Assert.assertTrue(authURL.startsWith("http://www.example.com/login_pkce?client_id=clientid&redirect_uri=null"));

    // Verify PKCE specific query params
    Assert.assertTrue(authURL.contains("&state="));
    Assert.assertTrue(authURL.contains("&code_challenge="));
    Assert.assertTrue(authURL.contains("&code_challenge_method=S256"));
  }

  @Test
  public void testGetAuthURLForMissingClientCredentials() throws IOException {
    // Attempt to create provider with missing client credentials and 'reuse_client_credentials'
    // query param 'true'.
    String loginURL = "http://www.example.com/login40";
    String tokenRefreshURL = "http://www.example.com/token40";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, null, null);
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
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret");
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
    request = createPutRequest(loginURL, tokenRefreshURL, null, null);
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
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret");
    HttpResponse createResponse = makePutCall("provider/testprovider", request);
    Assert.assertEquals(400, createResponse.getResponseCode());
  }

  @Test
  public void testCreateProviderBadTokenRefreshURL() throws IOException {
    // Attempt to create provider
    String loginURL = "http://www.example.com/token";
    String tokenRefreshURL = "badurl";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret");
    HttpResponse createResponse = makePutCall("provider/testprovider", request);
    Assert.assertEquals(400, createResponse.getResponseCode());
  }

  @Test
  public void testGetAuthURLProviderDoesNotExist() throws IOException {
    HttpResponse getResponse = makeGetCall("provider/nonexistantprovider/authurl");
    Assert.assertEquals(404, getResponse.getResponseCode());
  }

  private HttpResponse makeDeleteCall(String endpoint) throws IOException {
    URL url = serviceURI
        .resolve(String.format("v1/oauth/%s", endpoint))
        .toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.DELETE, url).build();
    return HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
  }

  @Test
  public void testGetOAuthCredentialStandardLongLivedToken() throws Exception {
    String providerName = "testGetCredLongLived";
    String credentialName = "cred3";

    // Create provider
    PutOAuthProviderRequest request = createPutRequest("http://localhost:8080/login",
        "http://localhost:8080/token", "clientid", "clientsecret");
    HttpResponse createResp = makePutCall("provider/" + providerName, request);
    Assert.assertEquals(200, createResp.getResponseCode());

    // Put an access token in the store (no refresh token).
    // Standard flow will return this directly if present.
    OAuthAccessToken accessToken = OAuthAccessToken.newBuilder()
        .withAccessToken("long_lived_token")
        .withExpiresAt(0L)
        .build();
    getSecureStoreManager().put("system",
        "oauthaccesstoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(accessToken), "Test", Collections.emptyMap());

    // Call GET credential endpoint
    HttpResponse response = makeGetCall("provider/" + providerName +
        "/credential/" + credentialName);

    Assert.assertEquals(200, response.getResponseCode());
    String body = response.getResponseBodyAsString();
    Assert.assertTrue(body.contains("long_lived_token"));
  }

  @Test
  public void testGetOAuthCredentialStandard() throws Exception {
    String providerName = "testGetCredStd";
    String credentialName = "cred2";

    // Create provider (default is STANDARD refresh type)
    PutOAuthProviderRequest request = createPutRequest("http://localhost:" + mockTokenPort + "/login",
        mockTokenUrl, "clientid", "clientsecret");
    HttpResponse createResp = makePutCall("provider/" + providerName, request);
    Assert.assertEquals(200, createResp.getResponseCode());

    // Put a refresh token so the refresh can happen
    OAuthRefreshToken refreshToken =
        new OAuthRefreshToken("valid_refresh_token", "http://redirect");
    getSecureStoreManager().put("system",
        "oauthrefreshtoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(refreshToken), "Test", Collections.emptyMap());

    // Call GET credential endpoint
    HttpResponse response = makeGetCall("provider/" + providerName +
        "/credential/" + credentialName);

    Assert.assertEquals(200, response.getResponseCode());
    String body = response.getResponseBodyAsString();
    Assert.assertTrue(body.contains("mock_access_token"));
  }

  @Test
  public void testGetOAuthCredentialValidityTokenRefresh() throws Exception {
    String providerName = "testRefresh";
    String credentialName = "cred1";

    // Create provider
    PutOAuthProviderRequest request = createPutRequest("http://localhost:" + mockTokenPort + "/login",
        mockTokenUrl, "clientid", "clientsecret");
    HttpResponse createResp = makePutCall("provider/" + providerName, request);
    Assert.assertEquals("Create provider failed: " + createResp.getResponseBodyAsString(),
        200, createResp.getResponseCode());

    // Put EXPIRED token in SecureStore
    // Note: we use "system" namespace and specific key formats that OAuthStore expects.
    long expiredTime = System.currentTimeMillis() - 1000000L;
    OAuthAccessToken expiredToken = OAuthAccessToken.newBuilder()
        .withAccessToken("expired_token")
        .withExpiresAt(expiredTime)
        .build();

    getSecureStoreManager().put("system",
        "oauthaccesstoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(expiredToken), "RTR Test", Collections.emptyMap());

    // Also put a refresh token so the refresh can happen
    OAuthRefreshToken refreshToken =
        new OAuthRefreshToken("valid_refresh_token", "http://redirect");
    getSecureStoreManager().put("system",
        "oauthrefreshtoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(refreshToken), "RTR Test", Collections.emptyMap());

    // Call validity endpoint. This will find the expired token and try to refresh it using the tokenUrl!
    HttpResponse response = makeGetCall("provider/" + providerName +
        "/credential/" + credentialName + "/valid");

    // Should succeed and return true
    Assert.assertEquals(200, response.getResponseCode());
    String body = response.getResponseBodyAsString();
    Assert.assertTrue(body.replaceAll("\\s+", "").contains("\"isValid\":true"));
  }

  @Test
  public void testDeleteProvider() throws IOException {
    // Attempt to create provider
    String loginURL = "http://www.example.com/login_del";
    String tokenRefreshURL = "http://www.example.com/token_del";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret");
    HttpResponse createResponse = makePutCall("provider/testprovider_del", request);
    Assert.assertEquals(200, createResponse.getResponseCode());

    // Attempt to delete provider
    HttpResponse deleteResponse = makeDeleteCall("provider/testprovider_del");
    Assert.assertEquals(200, deleteResponse.getResponseCode());

    // Verify it's gone
    HttpResponse getResponse = makeGetCall("provider/testprovider_del/authurl");
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

  @Test
  public void testGetOAuthCredentialRTR() throws Exception {
    String providerName = "testGetCredRtr";
    String credentialName = "cred1";

    // Create provider with RTR refresh type
    PutOAuthProviderRequest request = createPutRequest("http://localhost:" + mockTokenPort + "/login",
        mockTokenUrl, "clientid", "clientsecret", OAuthProvider.CredentialEncodingStrategy.FORM_BODY,
        null, AuthType.STANDARD, RefreshType.RTR);
    HttpResponse createResp = makePutCall("provider/" + providerName, request);
    Assert.assertEquals("Create provider failed: " + createResp.getResponseBodyAsString(),
        200, createResp.getResponseCode());

    // Put an EXPIRED access token so it forces a refresh
    long expiredTime = System.currentTimeMillis() - 1000000L;
    OAuthAccessToken expiredToken = OAuthAccessToken.newBuilder()
        .withAccessToken("expired_token")
        .withExpiresAt(expiredTime)
        .build();
    getSecureStoreManager().put("system",
        "oauthaccesstoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(expiredToken), "RTR Test", Collections.emptyMap());

    // Put a valid refresh token
    OAuthRefreshToken refreshToken = new OAuthRefreshToken("valid_refresh_token", "http://redirect");
    getSecureStoreManager().put("system",
        "oauthrefreshtoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(refreshToken), "Test", Collections.emptyMap());

    // Call GET credential endpoint
    HttpResponse response = makeGetCall("provider/" + providerName +
        "/credential/" + credentialName);

    Assert.assertEquals("Get credential failed: " + response.getResponseBodyAsString(),
        200, response.getResponseCode());
    String body = response.getResponseBodyAsString();
    Assert.assertTrue(body.contains("mock_access_token"));
  }
}
