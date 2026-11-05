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
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.datapipeline.oauth.AuthType;
import io.cdap.cdap.datapipeline.oauth.OAuthAccessToken;
import io.cdap.cdap.datapipeline.oauth.OAuthProvider;
import io.cdap.cdap.datapipeline.oauth.OAuthRefreshToken;
import io.cdap.cdap.datapipeline.oauth.OAuthStore;
import io.cdap.cdap.datapipeline.oauth.PutOAuthCredentialRequest;
import io.cdap.cdap.datapipeline.oauth.PutOAuthProviderRequest;
import io.cdap.cdap.datapipeline.oauth.RefreshType;
import io.cdap.cdap.datapipeline.service.OAuthHandler;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.store.SecureStoreService;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class OAuthServiceTest extends DataPipelineServiceTest {

  private static final Gson GSON = new GsonBuilder()
      .setPrettyPrinting()
      .create();
  public static final String ALICE_NAME = "alice";
  public static final Principal ALICE_PRINCIPAL = new Principal(ALICE_NAME, Principal.PrincipalType.USER);

  @ClassRule
  public static final TestConfiguration AUTH_CONFIG = new TestConfiguration(
      "security.auth.oauth.allow.private.urls", "true").enableAuthorization(TMP_FOLDER);

  private static final AtomicBoolean identityCalled = new AtomicBoolean(false);
  private static final AtomicBoolean redirectTargetCalled = new AtomicBoolean(false);
  private static volatile int mockServerPort;

  private NettyHttpService mockOAuthServer;
  private String mockTokenUrl;
  private int mockTokenPort;
  private String user;

  public static final class MockTokenHandler extends AbstractHttpHandler {
    @POST
    @Path("/token")
    public void token(FullHttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "{\"access_token\":\"mock_access_token\",\"expires_in\":3600}");
    }

    @POST
    @Path("/token-invalid")
    public void invalidToken(FullHttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
          "{\"error\":\"invalid_grant\",\"error_description\":\"expired access/refresh token\"}");
    }

    @POST
    @Path("/token-missing-fields")
    public void missingFieldsToken(FullHttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK,
          "{\"secret_leak\":\"SENSITIVE_UPSTREAM_BODY\"}");
    }

    @POST
    @Path("/token-empty")
    public void emptyToken(FullHttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "");
    }

    @POST
    @Path("/token-malformed-json")
    public void malformedJsonToken(FullHttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "{NOT_JSON_SECRET_PAYLOAD");
    }

    @POST
    @Path("/token-bad-identity")
    public void badIdentityToken(FullHttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK,
          "{\"access_token\":\"rtr_token_with_bad_id\",\"refresh_token\":\"rtr_refresh_token\","
              + "\"id\":\"http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token\"}");
    }

    @POST
    @Path("/token-mismatched-identity")
    public void mismatchedIdentityToken(FullHttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK,
          "{\"access_token\":\"rtr_token_with_mismatched_id\",\"refresh_token\":\"rtr_refresh_token\","
              + "\"id\":\"https://www.example.com/identity-capture\"}");
    }

    @POST
    @Path("/token-valid-identity")
    public void validIdentityToken(FullHttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK,
          "{\"access_token\":\"rtr_token_with_valid_id\",\"refresh_token\":\"rtr_refresh_token\","
              + "\"id\":\"http://127.0.0.1:" + mockServerPort + "/identity-capture\"}");
    }

    @GET
    @Path("/identity-capture")
    public void identityCapture(FullHttpRequest request, HttpResponder responder) {
      identityCalled.set(true);
      responder.sendString(HttpResponseStatus.OK, "{\"status\":\"ok\"}");
    }

    @GET
    @POST
    @Path("/redirect-to-internal")
    public void redirectToInternal(FullHttpRequest request, HttpResponder responder) {
      responder.sendStatus(HttpResponseStatus.FOUND,
          new DefaultHttpHeaders().set(HttpHeaderNames.LOCATION,
              "http://127.0.0.1:" + mockServerPort + "/internal-target"));
    }

    @GET
    @POST
    @Path("/internal-target")
    public void internalTarget(FullHttpRequest request, HttpResponder responder) {
      redirectTargetCalled.set(true);
      responder.sendString(HttpResponseStatus.OK,
          "{\"access_token\":\"leaked_internal_token\",\"expires_in\":3600}");
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
    user = null;
    setupMockSecureStoreLeasing();
    mockOAuthServer = NettyHttpService.builder("mock-oauth-server")
        .setHost("localhost")
        .setPort(0)
        .setHttpHandlers(new MockTokenHandler())
        .build();
    mockOAuthServer.start();
    mockTokenPort = mockOAuthServer.getBindAddress().getPort();
    mockServerPort = mockTokenPort;
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
    HttpRequest.Builder builder = HttpRequest.builder(HttpMethod.DELETE, url);
    if (user != null) {
      builder.addHeader(Constants.Security.Headers.USER_ID, user);
    }
    return HttpRequests.execute(builder.build(), new DefaultHttpRequestConfig(false));
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
    HttpRequest.Builder builder = HttpRequest.builder(HttpMethod.GET, url);
    if (user != null) {
      builder.addHeader(Constants.Security.Headers.USER_ID, user);
    }
    return HttpRequests.execute(builder.build(), new DefaultHttpRequestConfig(false));
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
    HttpRequest.Builder builder = HttpRequest.builder(HttpMethod.PUT, url)
        .withBody(GSON.toJson(body));
    if (user != null) {
      builder.addHeader(Constants.Security.Headers.USER_ID, user);
    }
    return HttpRequests.execute(builder.build(), new DefaultHttpRequestConfig(false));
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

  @Test
  public void testGetOAuthCredentialValidityInvalidTokenStandard() throws Exception {
    String providerName = "testInvalidStandard";
    String credentialName = "cred1";
    String invalidTokenUrl = "http://localhost:" + mockTokenPort + "/token-invalid";

    PutOAuthProviderRequest request = createPutRequest("http://localhost:" + mockTokenPort + "/login",
        invalidTokenUrl, "clientid", "clientsecret");
    HttpResponse createResp = makePutCall("provider/" + providerName, request);
    Assert.assertEquals("Create provider failed: " + createResp.getResponseBodyAsString(),
        200, createResp.getResponseCode());

    OAuthRefreshToken refreshToken = new OAuthRefreshToken("revoked_refresh_token", "http://redirect");
    getSecureStoreManager().put("system",
        "oauthrefreshtoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(refreshToken), "Standard Invalid Test", Collections.emptyMap());

    HttpResponse response = makeGetCall("provider/" + providerName +
        "/credential/" + credentialName + "/valid");

    Assert.assertEquals(200, response.getResponseCode());
    String body = response.getResponseBodyAsString();
    Assert.assertTrue(body.replaceAll("\\s+", "").contains("\"isValid\":false"));
  }

  @Test
  public void testGetOAuthCredentialValidityInvalidTokenRTR() throws Exception {
    String providerName = "testInvalidRtr";
    String credentialName = "cred1";
    String invalidTokenUrl = "http://localhost:" + mockTokenPort + "/token-invalid";

    PutOAuthProviderRequest request = createPutRequest("http://localhost:" + mockTokenPort + "/login",
        invalidTokenUrl, "clientid", "clientsecret", OAuthProvider.CredentialEncodingStrategy.FORM_BODY,
        null, AuthType.STANDARD, RefreshType.RTR);
    HttpResponse createResp = makePutCall("provider/" + providerName, request);
    Assert.assertEquals("Create provider failed: " + createResp.getResponseBodyAsString(),
        200, createResp.getResponseCode());

    OAuthRefreshToken refreshToken = new OAuthRefreshToken("revoked_refresh_token", "http://redirect");
    getSecureStoreManager().put("system",
        "oauthrefreshtoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(refreshToken), "RTR Invalid Test", Collections.emptyMap());

    HttpResponse response = makeGetCall("provider/" + providerName +
        "/credential/" + credentialName + "/valid");

    Assert.assertEquals(200, response.getResponseCode());
    String body = response.getResponseBodyAsString();
    Assert.assertTrue(body.replaceAll("\\s+", "").contains("\"isValid\":false"));
  }

  @Test
  public void testOAuthAuthorization() throws Exception {
    String providerName = "authProvider";
    String credentialName = "cred1";
    String loginUrl = "http://localhost:" + mockTokenPort + "/login";
    PutOAuthProviderRequest providerRequest =
        createPutRequest(loginUrl, mockTokenUrl, "clientid", "clientsecret");
    PutOAuthProviderRequest hijackedRequest =
        createPutRequest("http://attacker.example.com/login", mockTokenUrl, null, null);
    PutOAuthCredentialRequest credentialRequest =
        new PutOAuthCredentialRequest("mock_code", "http://redirect", null);

    // Seed provider as privileged user so we can verify unprivileged requests do not mutate or delete it
    HttpResponse seedResp = makePutCall("provider/" + providerName, providerRequest);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, seedResp.getResponseCode());

    Authorizable defaultNsAuthorizable = Authorizable.fromEntityId(NamespaceId.DEFAULT);
    Authorizable systemNsAuthorizable = Authorizable.fromEntityId(NamespaceId.SYSTEM);
    getPermissionManager().grant(defaultNsAuthorizable, ALICE_PRINCIPAL, EnumSet.of(StandardPermission.GET));
    getPermissionManager().grant(systemNsAuthorizable, ALICE_PRINCIPAL, EnumSet.of(StandardPermission.GET));
    try {
      user = ALICE_NAME;

      // Verify all 6 endpoints return 403 Forbidden for unprivileged user alice
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
          makeGetCall("provider/" + providerName + "/authurl").getResponseCode());
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
          makePutCall("provider/" + providerName, providerRequest).getResponseCode());
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
          makePutCall("provider/" + providerName + "?reuse_client_credentials=true",
              hijackedRequest).getResponseCode());
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
          makeDeleteCall("provider/" + providerName).getResponseCode());
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
          makeDeleteCall("provider/" + providerName + "?preserve_client_credentials=true")
              .getResponseCode());
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
          makePutCall("provider/" + providerName + "/credential/" + credentialName,
              credentialRequest).getResponseCode());
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
          makeGetCall("provider/" + providerName + "/credential/" + credentialName)
              .getResponseCode());
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
          makeGetCall("provider/" + providerName + "/credential/" + credentialName + "/valid")
              .getResponseCode());

      // Verify StructuredTable row was not modified by reuse_client_credentials=true
      // and not deleted by preserve_client_credentials=true
      user = null;
      HttpResponse verifyUnmodifiedResp = makeGetCall("provider/" + providerName + "/authurl");
      Assert.assertEquals(HttpURLConnection.HTTP_OK, verifyUnmodifiedResp.getResponseCode());
      Assert.assertEquals(loginUrl + "?client_id=clientid&redirect_uri=null",
          verifyUnmodifiedResp.getResponseBodyAsString());

      // Grant alice SecureKeyId permissions progressively (GET -> UPDATE -> DELETE)
      user = ALICE_NAME;
      Authorizable[] secureKeyAuthorizables = new Authorizable[] {
          Authorizable.fromEntityId(NamespaceId.SYSTEM, EntityType.SECUREKEY),
          Authorizable.fromEntityId(
              NamespaceId.SYSTEM.secureKey(OAuthStore.getClientCredsKey(providerName))),
          Authorizable.fromEntityId(
              NamespaceId.SYSTEM.secureKey(OAuthStore.getRefreshTokenKey(providerName, credentialName))),
          Authorizable.fromEntityId(NamespaceId.SYSTEM.secureKey(
              "oauthaccesstoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase()))
      };
      EnumSet<StandardPermission> allSecureKeyPerms = EnumSet.of(
          StandardPermission.GET, StandardPermission.UPDATE, StandardPermission.DELETE);
      try {
        // 1. With only GET on SecureKeyId: getAuthURL succeeds, but PUT and DELETE endpoints still return 403
        for (Authorizable authorizable : secureKeyAuthorizables) {
          getPermissionManager().grant(authorizable, ALICE_PRINCIPAL, EnumSet.of(StandardPermission.GET));
        }
        Assert.assertEquals(HttpURLConnection.HTTP_OK,
            makeGetCall("provider/" + providerName + "/authurl").getResponseCode());
        Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
            makePutCall("provider/" + providerName, providerRequest).getResponseCode());
        Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
            makePutCall("provider/" + providerName + "?reuse_client_credentials=true",
                hijackedRequest).getResponseCode());
        Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
            makePutCall("provider/" + providerName + "/credential/" + credentialName,
                credentialRequest).getResponseCode());
        Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
            makeDeleteCall("provider/" + providerName).getResponseCode());
        Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
            makeDeleteCall("provider/" + providerName + "?preserve_client_credentials=true")
                .getResponseCode());

        // Verify reuse_client_credentials=true did not overwrite loginURL even when caller has GET on ClientCredsKey
        HttpResponse verifyAfterGetGrantResp = makeGetCall("provider/" + providerName + "/authurl");
        Assert.assertEquals(HttpURLConnection.HTTP_OK, verifyAfterGetGrantResp.getResponseCode());
        Assert.assertEquals(loginUrl + "?client_id=clientid&redirect_uri=null",
            verifyAfterGetGrantResp.getResponseBodyAsString());

        // 2. With GET + UPDATE on SecureKeyId: PUT and GET endpoints succeed, but DELETE still returns 403
        for (Authorizable authorizable : secureKeyAuthorizables) {
          getPermissionManager().grant(authorizable, ALICE_PRINCIPAL, EnumSet.of(StandardPermission.UPDATE));
        }
        Assert.assertEquals(HttpURLConnection.HTTP_OK,
            makePutCall("provider/" + providerName, providerRequest).getResponseCode());
        Assert.assertEquals(HttpURLConnection.HTTP_OK,
            makePutCall("provider/" + providerName + "/credential/" + credentialName,
                credentialRequest).getResponseCode());

        HttpResponse credResp =
            makeGetCall("provider/" + providerName + "/credential/" + credentialName);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, credResp.getResponseCode());
        Assert.assertTrue(credResp.getResponseBodyAsString().contains("mock_access_token"));

        HttpResponse validResp =
            makeGetCall("provider/" + providerName + "/credential/" + credentialName + "/valid");
        Assert.assertEquals(HttpURLConnection.HTTP_OK, validResp.getResponseCode());
        Assert.assertTrue(validResp.getResponseBodyAsString()
            .replaceAll("\\s+", "").contains("\"isValid\":true"));

        Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
            makeDeleteCall("provider/" + providerName).getResponseCode());
        Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
            makeDeleteCall("provider/" + providerName + "?preserve_client_credentials=true")
                .getResponseCode());

        // 3. With GET + UPDATE + DELETE on SecureKeyId: DELETE succeeds
        for (Authorizable authorizable : secureKeyAuthorizables) {
          getPermissionManager().grant(authorizable, ALICE_PRINCIPAL, EnumSet.of(StandardPermission.DELETE));
        }
        Assert.assertEquals(HttpURLConnection.HTTP_OK,
            makeDeleteCall("provider/" + providerName).getResponseCode());
      } finally {
        for (Authorizable authorizable : secureKeyAuthorizables) {
          getPermissionManager().revoke(authorizable, ALICE_PRINCIPAL, allSecureKeyPerms);
        }
      }
    } finally {
      user = null;
      getPermissionManager().revoke(defaultNsAuthorizable, ALICE_PRINCIPAL, EnumSet.of(StandardPermission.GET));
      getPermissionManager().revoke(systemNsAuthorizable, ALICE_PRINCIPAL, EnumSet.of(StandardPermission.GET));
    }
  }

  @Test
  public void testOAuthUrlSsrfProtection() throws Exception {
    // Seed a provider with a loopback tokenRefreshURL while private URLs are allowed so we can test
    // runtime buildHttpRequest SSRF blocking when private URLs are subsequently disallowed.
    String runtimeProvider = "testRuntimeSsrf";
    String runtimeCred = "cred1";
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        makePutCall("provider/" + runtimeProvider,
            createPutRequest("http://localhost:" + mockTokenPort + "/login",
                mockTokenUrl, "clientid", "clientsecret")).getResponseCode());
    getSecureStoreManager().put("system",
        "oauthrefreshtoken-" + runtimeProvider.toLowerCase() + "-" + runtimeCred.toLowerCase(),
        GSON.toJson(new OAuthRefreshToken("valid_refresh_token", "http://redirect")),
        "Runtime SSRF Test", Collections.emptyMap());

    String validUrl = "https://8.8.8.8/oauth";
    String[] alwaysDisallowedUrls = new String[] {
        "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token",
        "http://0.0.0.0/token",
        "http://[::]/token",
        "http://224.0.0.1/token",
        "http://[ff02::1]/token",
        "http://[fe80::1]/token",
        "http://[fe80::1%25lo]/token",
        "http://[::1%25lo]:8080/token",
        "http://[::169.254.169.254]/token",
        "http://[::ffff:169.254.169.254]/token",
        "http://[2002:a9fe:a9fe::1]/token",
        "http://[2001:0000:4136:e378:8000:63bf:5601:5601]/token",
        "http:///token",
        "http://.../token",
        "http://bad_host.example.com/token",
        "file:///etc/passwd",
        "ftp://example.com/token",
        "https://user:pass@example.com/token"
    };
    String[] privateOrLoopbackUrls = new String[] {
        "http://10.0.0.1/token",
        "http://172.16.0.1/token",
        "http://192.168.1.1/token",
        "http://127.0.0.1:8080/token",
        "http://localhost:8080/token",
        "http://[::1]:8080/token",
        "http://100.64.0.1/token",
        "http://[fc00::1]/token",
        "http://[fd00::1]/token",
        "http://[2002:a00:1::]/token",
        "http://[2001:0000:4136:e378:8000:63bf:f5ff:fffe]/token",
        "http://[::10.0.0.1]/token",
        "http://[::ffff:10.0.0.1]/token"
    };

    OAuthHandler.setAllowPrivateUrlsForTest(false);
    try {
      for (String[] urlGroup : new String[][] {alwaysDisallowedUrls, privateOrLoopbackUrls}) {
        for (String badUrl : urlGroup) {
          PutOAuthProviderRequest badRefreshReq = createPutRequest(validUrl, badUrl, "clientid", "clientsecret");
          HttpResponse badRefreshResp = makePutCall("provider/ssrf_provider", badRefreshReq);
          Assert.assertEquals("Expected 400 for tokenRefreshURL: " + badUrl,
              HttpURLConnection.HTTP_BAD_REQUEST, badRefreshResp.getResponseCode());

          PutOAuthProviderRequest badLoginReq = createPutRequest(badUrl, validUrl, "clientid", "clientsecret");
          HttpResponse badLoginResp = makePutCall("provider/ssrf_provider", badLoginReq);
          Assert.assertEquals("Expected 400 for loginURL: " + badUrl,
              HttpURLConnection.HTTP_BAD_REQUEST, badLoginResp.getResponseCode());
        }
      }

      // Public literal passes validation even when allowPrivate=false
      Assert.assertEquals(HttpURLConnection.HTTP_OK,
          makePutCall("provider/ssrf_public_literal",
              createPutRequest("https://8.8.8.8/login", "https://8.8.8.8/token", "clientid", "clientsecret"))
              .getResponseCode());

      // Runtime buildHttpRequest SSRF verification when executing token exchange or refresh
      PutOAuthCredentialRequest putCredReq =
          new PutOAuthCredentialRequest("one_time_code", "http://redirect", null);
      Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST,
          makePutCall("provider/" + runtimeProvider + "/credential/" + runtimeCred, putCredReq)
              .getResponseCode());
      Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST,
          makeGetCall("provider/" + runtimeProvider + "/credential/" + runtimeCred)
              .getResponseCode());
    } finally {
      OAuthHandler.setAllowPrivateUrlsForTest(null);
    }

    // With allowPrivate=true (test configuration default), link-local/metadata, wildcard, multicast,
    // invalid scheme, and userinfo URLs remain rejected, while private/loopback URLs are accepted.
    for (String badUrl : alwaysDisallowedUrls) {
      Assert.assertEquals("Expected 400 even with allowPrivate=true for: " + badUrl,
          HttpURLConnection.HTTP_BAD_REQUEST,
          makePutCall("provider/ssrf_always_blocked",
              createPutRequest(validUrl, badUrl, "clientid", "clientsecret")).getResponseCode());
    }
    int idx = 0;
    for (String privateUrl : privateOrLoopbackUrls) {
      Assert.assertEquals("Expected 200 with allowPrivate=true for: " + privateUrl,
          HttpURLConnection.HTTP_OK,
          makePutCall("provider/ssrf_private_allowed_" + (idx++),
              createPutRequest(privateUrl, privateUrl, "clientid", "clientsecret")).getResponseCode());
    }

    // Verify identityUrl host/port/scheme mismatch or SSRF address is rejected without sending Bearer token
    identityCalled.set(false);
    String providerName = "testIdentityUrlSsrf";
    String credentialName = "cred1";
    PutOAuthProviderRequest providerReq = createPutRequest(
        "http://127.0.0.1:" + mockTokenPort + "/login",
        "http://127.0.0.1:" + mockTokenPort + "/token",
        "clientid", "clientsecret",
        OAuthProvider.CredentialEncodingStrategy.FORM_BODY, null, AuthType.STANDARD, RefreshType.RTR);
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        makePutCall("provider/" + providerName, providerReq).getResponseCode());

    String[] badIdentityUrls = new String[] {
        "http://localhost:" + mockTokenPort + "/identity-capture",
        "https://127.0.0.1:" + mockTokenPort + "/identity-capture",
        "http://127.0.0.1:" + (mockTokenPort == 65535 ? mockTokenPort - 1 : mockTokenPort + 1) + "/identity-capture",
        "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
    };
    for (String badIdentityUrl : badIdentityUrls) {
      OAuthAccessToken badToken = OAuthAccessToken.newBuilder()
          .withAccessToken("sensitive_bearer_token")
          .withExpiresAt(0L)
          .withIdentityUrl(badIdentityUrl)
          .build();
      getSecureStoreManager().put("system",
          "oauthaccesstoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
          GSON.toJson(badToken), "Identity SSRF Test", Collections.emptyMap());

      // No refresh token seeded -> validity check must reject the cached token's identityUrl without calling it
      // and then fail with 404 when falling back to missing refresh token
      HttpResponse validResp = makeGetCall("provider/" + providerName + "/credential/" + credentialName + "/valid");
      Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, validResp.getResponseCode());
      Assert.assertFalse("Disallowed identityUrl should not be contacted: " + badIdentityUrl,
          identityCalled.get());
    }

    // Positive identityUrl verification: provider with distinct loginURL and tokenRefreshURL origins
    // accepts identityUrl matching either origin and calls the identity endpoint
    String distinctProvider = "testDistinctIdentityOrigins";
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        makePutCall("provider/" + distinctProvider,
            createPutRequest("http://127.0.0.1:" + mockTokenPort + "/login",
                "http://localhost:" + mockTokenPort + "/token",
                "clientid", "clientsecret",
                OAuthProvider.CredentialEncodingStrategy.FORM_BODY, null,
                AuthType.STANDARD, RefreshType.RTR)).getResponseCode());

    for (String allowedIdUrl : new String[] {
        "http://127.0.0.1:" + mockTokenPort + "/identity-capture",
        "http://localhost:" + mockTokenPort + "/identity-capture"
    }) {
      identityCalled.set(false);
      OAuthAccessToken validIdToken = OAuthAccessToken.newBuilder()
          .withAccessToken("valid_bearer_token")
          .withExpiresAt(0L)
          .withIdentityUrl(allowedIdUrl)
          .build();
      getSecureStoreManager().put("system",
          "oauthaccesstoken-" + distinctProvider.toLowerCase() + "-" + credentialName.toLowerCase(),
          GSON.toJson(validIdToken), "Valid Identity Origin Test", Collections.emptyMap());

      HttpResponse validResp =
          makeGetCall("provider/" + distinctProvider + "/credential/" + credentialName + "/valid");
      Assert.assertEquals(HttpURLConnection.HTTP_OK, validResp.getResponseCode());
      Assert.assertTrue(validResp.getResponseBodyAsString().replaceAll("\\s+", "").contains("\"isValid\":true"));
      Assert.assertTrue("Expected identityUrl to be contacted: " + allowedIdUrl, identityCalled.get());
    }

    // Verify writeAccessToken strips disallowed/mismatched identityUrl before saving to SecureStore,
    // while preserving a valid same-origin identityUrl
    for (String badEndpoint : new String[] {"/token-bad-identity", "/token-mismatched-identity"}) {
      String badIdSaveProvider = "testBadIdentityNotPersisted_" + badEndpoint.substring(1);
      Assert.assertEquals(HttpURLConnection.HTTP_OK,
          makePutCall("provider/" + badIdSaveProvider,
              createPutRequest("http://localhost:" + mockTokenPort + "/login",
                  "http://localhost:" + mockTokenPort + badEndpoint,
                  "clientid", "clientsecret",
                  OAuthProvider.CredentialEncodingStrategy.FORM_BODY, null,
                  AuthType.STANDARD, RefreshType.RTR)).getResponseCode());
      Assert.assertEquals(HttpURLConnection.HTTP_OK,
          makePutCall("provider/" + badIdSaveProvider + "/credential/" + credentialName,
              new PutOAuthCredentialRequest("one_time_code", "http://redirect", null)).getResponseCode());
      String storedTokenJson = new String(
          getSecureStore().getData("system",
              "oauthaccesstoken-" + badIdSaveProvider.toLowerCase() + "-" + credentialName.toLowerCase()),
          StandardCharsets.UTF_8);
      OAuthAccessToken storedToken = GSON.fromJson(storedTokenJson, OAuthAccessToken.class);
      Assert.assertNull("Expected disallowed/mismatched identityUrl to be stripped for " + badEndpoint,
          storedToken.getIdentityUrl());
    }

    String validIdSaveProvider = "testValidIdentityPersisted";
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        makePutCall("provider/" + validIdSaveProvider,
            createPutRequest("http://127.0.0.1:" + mockTokenPort + "/login",
                "http://localhost:" + mockTokenPort + "/token-valid-identity",
                "clientid", "clientsecret",
                OAuthProvider.CredentialEncodingStrategy.FORM_BODY, null,
                AuthType.STANDARD, RefreshType.RTR)).getResponseCode());
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        makePutCall("provider/" + validIdSaveProvider + "/credential/" + credentialName,
            new PutOAuthCredentialRequest("one_time_code", "http://redirect", null)).getResponseCode());
    String storedValidTokenJson = new String(
        getSecureStore().getData("system",
            "oauthaccesstoken-" + validIdSaveProvider.toLowerCase() + "-" + credentialName.toLowerCase()),
        StandardCharsets.UTF_8);
    OAuthAccessToken storedValidToken = GSON.fromJson(storedValidTokenJson, OAuthAccessToken.class);
    Assert.assertEquals("http://127.0.0.1:" + mockTokenPort + "/identity-capture",
        storedValidToken.getIdentityUrl());

    // Verify 302 redirects on all 3 outbound OAuth request paths (putOAuthCredential, isAccessTokenValid,
    // and executeTokenRefresh) are NOT followed
    redirectTargetCalled.set(false);
    String redirectProvider = "testNoFollowRedirect";
    String redirectUrl = "http://localhost:" + mockTokenPort + "/redirect-to-internal";
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        makePutCall("provider/" + redirectProvider,
            createPutRequest("http://localhost:" + mockTokenPort + "/login",
                redirectUrl, "clientid", "clientsecret",
                OAuthProvider.CredentialEncodingStrategy.FORM_BODY, null,
                AuthType.STANDARD, RefreshType.RTR)).getResponseCode());
    HttpResponse redirectPutResp = makePutCall("provider/" + redirectProvider + "/credential/" + credentialName,
        new PutOAuthCredentialRequest("one_time_code", "http://redirect", null));
    Assert.assertEquals(HttpURLConnection.HTTP_MOVED_TEMP, redirectPutResp.getResponseCode());
    Assert.assertFalse("Outbound token exchange must not follow HTTP 302 redirect", redirectTargetCalled.get());

    OAuthAccessToken redirectingIdToken = OAuthAccessToken.newBuilder()
        .withAccessToken("sensitive_bearer_token")
        .withExpiresAt(0L)
        .withIdentityUrl(redirectUrl)
        .build();
    getSecureStoreManager().put("system",
        "oauthaccesstoken-" + redirectProvider.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(redirectingIdToken), "Redirect Identity Test", Collections.emptyMap());
    getSecureStoreManager().put("system",
        "oauthrefreshtoken-" + redirectProvider.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(new OAuthRefreshToken("valid_refresh_token", "http://redirect")),
        "Redirect Refresh Test", Collections.emptyMap());
    HttpResponse redirectGetResp =
        makeGetCall("provider/" + redirectProvider + "/credential/" + credentialName);
    Assert.assertEquals(HttpURLConnection.HTTP_MOVED_TEMP, redirectGetResp.getResponseCode());
    Assert.assertFalse("Outbound token refresh must not follow HTTP 302 redirect", redirectTargetCalled.get());

    HttpResponse redirectValidResp =
        makeGetCall("provider/" + redirectProvider + "/credential/" + credentialName + "/valid");
    Assert.assertEquals(HttpURLConnection.HTTP_OK, redirectValidResp.getResponseCode());
    Assert.assertTrue(redirectValidResp.getResponseBodyAsString()
        .replaceAll("\\s+", "").contains("\"isValid\":false"));
    Assert.assertFalse("Identity validation and refresh must not follow HTTP 302 redirect",
        redirectTargetCalled.get());
  }

  @Test
  public void testReuseClientCredentialsRejectsHostChange() throws Exception {
    String providerName = "testReuseHostChange";
    String initialLoginUrl = "http://www.example.com/login";
    String initialTokenUrl = "http://www.example.com/token";
    PutOAuthProviderRequest createReq =
        createPutRequest(initialLoginUrl, initialTokenUrl, "clientid", "clientsecret");
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        makePutCall("provider/" + providerName, createReq).getResponseCode());

    // Attempting to redirect tokenRefreshURL to a different host with reuse_client_credentials=true must fail (400)
    PutOAuthProviderRequest redirectTokenHostReq =
        createPutRequest(initialLoginUrl, "http://attacker.example.com/token", null, null);
    HttpResponse redirectTokenResp =
        makePutCall("provider/" + providerName + "?reuse_client_credentials=true", redirectTokenHostReq);
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, redirectTokenResp.getResponseCode());

    // Attempting to redirect loginURL to a different host or scheme with reuse_client_credentials=true must fail (400)
    PutOAuthProviderRequest redirectLoginHostReq =
        createPutRequest("https://www.example.com/login", initialTokenUrl, null, null);
    HttpResponse redirectLoginResp =
        makePutCall("provider/" + providerName + "?reuse_client_credentials=true", redirectLoginHostReq);
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, redirectLoginResp.getResponseCode());

    // Verify stored loginURL was not modified
    HttpResponse authUrlResp = makeGetCall("provider/" + providerName + "/authurl");
    Assert.assertEquals(HttpURLConnection.HTTP_OK, authUrlResp.getResponseCode());
    Assert.assertEquals("http://www.example.com/login?client_id=clientid&redirect_uri=null",
        authUrlResp.getResponseBodyAsString());

    // Two-step DELETE ?preserve_client_credentials=true followed by PUT ?reuse_client_credentials=true
    // with a changed host/scheme/port must ALSO be rejected with 400
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        makeDeleteCall("provider/" + providerName + "?preserve_client_credentials=true").getResponseCode());
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
        makeGetCall("provider/" + providerName + "/authurl").getResponseCode());

    HttpResponse hijackAfterDeleteResp =
        makePutCall("provider/" + providerName + "?reuse_client_credentials=true", redirectTokenHostReq);
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, hijackAfterDeleteResp.getResponseCode());

    // Recreating with the original origin and reuse_client_credentials=true succeeds (200)
    PutOAuthProviderRequest sameOriginUpdateReq =
        createPutRequest("http://www.example.com/v2/login", "http://www.example.com/v2/token", null, null);
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        makePutCall("provider/" + providerName + "?reuse_client_credentials=true", sameOriginUpdateReq)
            .getResponseCode());
  }

  @Test
  public void testErrorResponseDoesNotLeakUpstreamBody() throws Exception {
    String invalidProvider = "testNoLeakInvalid";
    String missingFieldsProvider = "testNoLeakMissing";
    String emptyBodyProvider = "testNoLeakEmpty";
    String malformedJsonProvider = "testNoLeakMalformed";
    String credentialName = "cred1";
    String invalidTokenUrl = "http://localhost:" + mockTokenPort + "/token-invalid";
    String missingFieldsUrl = "http://localhost:" + mockTokenPort + "/token-missing-fields";
    String emptyTokenUrl = "http://localhost:" + mockTokenPort + "/token-empty";
    String malformedJsonUrl = "http://localhost:" + mockTokenPort + "/token-malformed-json";

    Assert.assertEquals(HttpURLConnection.HTTP_OK, makePutCall("provider/" + invalidProvider,
        createPutRequest("http://localhost:" + mockTokenPort + "/login", invalidTokenUrl,
            "clientid", "clientsecret")).getResponseCode());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, makePutCall("provider/" + missingFieldsProvider,
        createPutRequest("http://localhost:" + mockTokenPort + "/login", missingFieldsUrl,
            "clientid", "clientsecret")).getResponseCode());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, makePutCall("provider/" + emptyBodyProvider,
        createPutRequest("http://localhost:" + mockTokenPort + "/login", emptyTokenUrl,
            "clientid", "clientsecret")).getResponseCode());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, makePutCall("provider/" + malformedJsonProvider,
        createPutRequest("http://localhost:" + mockTokenPort + "/login", malformedJsonUrl,
            "clientid", "clientsecret")).getResponseCode());

    // 1. putOAuthCredential against non-200 endpoint preserves status/message prefix but omits upstream body
    PutOAuthCredentialRequest putCredReq = new PutOAuthCredentialRequest("one_time_code", "http://redirect", null);
    HttpResponse putInvalidResp =
        makePutCall("provider/" + invalidProvider + "/credential/" + credentialName, putCredReq);
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, putInvalidResp.getResponseCode());
    String putInvalidBody = putInvalidResp.getResponseBodyAsString();
    Assert.assertTrue(putInvalidBody.contains("Request for refresh token did not return 200. Response code: 400"));
    Assert.assertFalse(putInvalidBody.contains("expired access/refresh token"));

    // 2. putOAuthCredential against 200 endpoint with missing token fields omits upstream body
    HttpResponse putMissingResp =
        makePutCall("provider/" + missingFieldsProvider + "/credential/" + credentialName, putCredReq);
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, putMissingResp.getResponseCode());
    String putMissingBody = putMissingResp.getResponseBodyAsString();
    Assert.assertTrue(
        putMissingBody.contains("Refresh token response is missing the required access token or refresh token."));
    Assert.assertFalse(putMissingBody.contains("SENSITIVE_UPSTREAM_BODY"));

    // 3. putOAuthCredential against 200 endpoint with empty body or malformed JSON returns 400 without leaking body
    HttpResponse putEmptyResp =
        makePutCall("provider/" + emptyBodyProvider + "/credential/" + credentialName, putCredReq);
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, putEmptyResp.getResponseCode());

    HttpResponse putMalformedResp =
        makePutCall("provider/" + malformedJsonProvider + "/credential/" + credentialName, putCredReq);
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, putMalformedResp.getResponseCode());
    String putMalformedBody = putMalformedResp.getResponseBodyAsString();
    Assert.assertTrue(putMalformedBody.contains("Refresh token response is not valid JSON."));
    Assert.assertFalse(putMalformedBody.contains("NOT_JSON_SECRET_PAYLOAD"));

    // 4. getOAuthCredential against non-200 endpoint preserves status/message prefix but omits upstream body
    OAuthRefreshToken refreshToken = new OAuthRefreshToken("revoked_refresh_token", "http://redirect");
    getSecureStoreManager().put("system",
        "oauthrefreshtoken-" + invalidProvider.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(refreshToken), "No Leak Test", Collections.emptyMap());
    HttpResponse getInvalidResp = makeGetCall("provider/" + invalidProvider + "/credential/" + credentialName);
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, getInvalidResp.getResponseCode());
    String getInvalidBody = getInvalidResp.getResponseBodyAsString();
    Assert.assertTrue(getInvalidBody.contains("Request for access token did not return 200. Response code: 400"));
    Assert.assertFalse(getInvalidBody.contains("expired access/refresh token"));

    // 5. getOAuthCredential against 200 endpoint with missing access_token omits upstream body
    getSecureStoreManager().put("system",
        "oauthrefreshtoken-" + missingFieldsProvider.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(refreshToken), "No Leak Missing Test", Collections.emptyMap());
    HttpResponse getMissingResp = makeGetCall("provider/" + missingFieldsProvider + "/credential/" + credentialName);
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, getMissingResp.getResponseCode());
    String getMissingBody = getMissingResp.getResponseBodyAsString();
    Assert.assertTrue(getMissingBody.contains("Access token response body does not have access token."));
    Assert.assertFalse(getMissingBody.contains("SENSITIVE_UPSTREAM_BODY"));

    // 6. getOAuthCredential and /valid against 200 endpoint with empty body or malformed JSON
    for (String badJsonProvider : new String[] {emptyBodyProvider, malformedJsonProvider}) {
      getSecureStoreManager().put("system",
          "oauthrefreshtoken-" + badJsonProvider.toLowerCase() + "-" + credentialName.toLowerCase(),
          GSON.toJson(refreshToken), "No Leak Bad JSON Test", Collections.emptyMap());
      HttpResponse getBadJsonResp = makeGetCall("provider/" + badJsonProvider + "/credential/" + credentialName);
      Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, getBadJsonResp.getResponseCode());
      Assert.assertFalse(getBadJsonResp.getResponseBodyAsString().contains("NOT_JSON_SECRET_PAYLOAD"));

      HttpResponse validBadJsonResp =
          makeGetCall("provider/" + badJsonProvider + "/credential/" + credentialName + "/valid");
      Assert.assertEquals(HttpURLConnection.HTTP_OK, validBadJsonResp.getResponseCode());
      Assert.assertTrue(validBadJsonResp.getResponseBodyAsString()
          .replaceAll("\\s+", "").contains("\"isValid\":false"));
    }
  }
}
