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

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.security.store.SecureStoreInfo;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.datapipeline.oauth.AuthType;
import io.cdap.cdap.datapipeline.oauth.OAuthAccessToken;
import io.cdap.cdap.datapipeline.oauth.OAuthProvider;
import io.cdap.cdap.datapipeline.oauth.OAuthRefreshToken;
import io.cdap.cdap.datapipeline.oauth.OAuthUrlValidator;
import io.cdap.cdap.datapipeline.oauth.PutOAuthCredentialRequest;
import io.cdap.cdap.datapipeline.oauth.PutOAuthProviderRequest;
import io.cdap.cdap.datapipeline.oauth.RefreshType;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.store.SecureStoreService;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.ServiceManager;
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
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class OAuthServiceTest extends DataPipelineServiceTest {

  private static final Gson GSON = new GsonBuilder()
      .setPrettyPrinting()
      .create();
  private static final String ALLOWED_HOSTS_CONF = "security.auth.oauth.allowed.hosts";
  private static final String TEST_DOMAIN = "oauth.example.invalid";

  @ClassRule
  public static final TestConfiguration AUTH_CONFIG = new TestConfiguration().enableAuthorization(TMP_FOLDER);

  private static ServiceManager loopbackServiceManager;
  private static URI loopbackServiceURI;

  private NettyHttpService mockOAuthServer;
  private MockTokenHandler mockTokenHandler;
  private String mockTokenUrl;
  private int mockTokenPort;
  private String user;

  public static final class MockTokenHandler extends AbstractHttpHandler {
    private final AtomicReference<String> lastAuthHeader = new AtomicReference<>();
    private final AtomicReference<String> lastContentType = new AtomicReference<>();
    private final AtomicReference<String> lastBody = new AtomicReference<>();
    private final AtomicInteger secretHits = new AtomicInteger();
    private final AtomicInteger identityHits = new AtomicInteger();
    private volatile String goodIdentityUrl = "";

    @POST
    @Path("/token")
    public void token(FullHttpRequest request, HttpResponder responder) {
      lastAuthHeader.set(request.headers().get(HttpHeaderNames.AUTHORIZATION));
      lastContentType.set(request.headers().get(HttpHeaderNames.CONTENT_TYPE));
      lastBody.set(request.content().toString(StandardCharsets.UTF_8));
      responder.sendString(HttpResponseStatus.OK, "{\"access_token\":\"mock_access_token\",\"expires_in\":3600}");
    }

    @POST
    @Path("/token-invalid")
    public void invalidToken(FullHttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
          "{\"error\":\"invalid_grant\",\"error_description\":\"expired access/refresh token\"}");
    }

    @POST
    @Path("/token-redirect")
    public void redirectToken(FullHttpRequest request, HttpResponder responder) {
      DefaultHttpHeaders headers = new DefaultHttpHeaders();
      headers.set(HttpHeaderNames.LOCATION, "/secret");
      responder.sendStatus(HttpResponseStatus.FOUND, headers);
    }

    @GET
    @Path("/secret")
    public void secretGet(FullHttpRequest request, HttpResponder responder) {
      secretHits.incrementAndGet();
      responder.sendString(HttpResponseStatus.OK, "{\"access_token\":\"leaked\"}");
    }

    @POST
    @Path("/secret")
    public void secretPost(FullHttpRequest request, HttpResponder responder) {
      secretHits.incrementAndGet();
      responder.sendString(HttpResponseStatus.OK, "{\"access_token\":\"leaked\"}");
    }

    @POST
    @Path("/token-bad-identity")
    public void badIdentityToken(FullHttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK,
          "{\"access_token\":\"tok_bad_id\",\"refresh_token\":\"ref_bad_id\","
              + "\"id\":\"http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token\"}");
    }

    @POST
    @Path("/token-good-identity")
    public void goodIdentityToken(FullHttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK,
          "{\"access_token\":\"tok_good_id\",\"refresh_token\":\"ref_good_id\","
              + "\"id\":\"" + goodIdentityUrl + "\"}");
    }

    @GET
    @Path("/identity")
    public void identity(FullHttpRequest request, HttpResponder responder) {
      identityHits.incrementAndGet();
      responder.sendString(HttpResponseStatus.OK, "{\"active\":true}");
    }

    @GET
    @Path("/identity-redirect")
    public void identityRedirect(FullHttpRequest request, HttpResponder responder) {
      DefaultHttpHeaders headers = new DefaultHttpHeaders();
      headers.set(HttpHeaderNames.LOCATION, "/secret");
      responder.sendStatus(HttpResponseStatus.FOUND, headers);
    }
  }

  @BeforeClass
  public static void setupMockSecureStoreLeasingAndLoopbackService() throws Exception {
    for (Object inst : new Object[] {getSecureStore(), getSecureStoreManager()}) {
      wrapSecureStoreService(inst);
    }
    ApplicationManager appManager = deployApplication(NamespaceId.SYSTEM, LoopbackOAuthApp.class);
    loopbackServiceManager = appManager.getServiceManager(LoopbackOAuthApp.SERVICE_NAME);
    loopbackServiceManager.startAndWaitForGoodRun(ProgramRunStatus.RUNNING, 2, TimeUnit.MINUTES);
    loopbackServiceURI = loopbackServiceManager.getServiceURL(1, TimeUnit.MINUTES).toURI();
  }

  @AfterClass
  public static void teardownLoopbackService() throws Exception {
    if (loopbackServiceManager != null) {
      loopbackServiceManager.stop();
      loopbackServiceManager.waitForStopped(2, TimeUnit.MINUTES);
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
    getConfiguration().set(ALLOWED_HOSTS_CONF, "");
    for (Object inst : new Object[] {getSecureStore(), getSecureStoreManager()}) {
      wrapSecureStoreService(inst);
    }
    mockTokenHandler = new MockTokenHandler();
    mockOAuthServer = NettyHttpService.builder("mock-oauth-server")
        .setHost("127.0.0.1")
        .setPort(0)
        .setHttpHandlers(mockTokenHandler)
        .build();
    mockOAuthServer.start();
    mockTokenPort = mockOAuthServer.getBindAddress().getPort();
    mockTokenUrl = "http://127.0.0.1:" + mockTokenPort + "/token";
    mockTokenHandler.goodIdentityUrl = "http://127.0.0.1:" + mockTokenPort + "/identity";
  }

  @After
  public void tearDownMockServer() throws Exception {
    getConfiguration().set(ALLOWED_HOSTS_CONF, "");
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
  public void testDefaultConfigurationHasEmptyAllowedHostsAndNoPrivateBypass() {
    CConfiguration defaultConf = CConfiguration.create();
    Assert.assertTrue(Strings.isNullOrEmpty(defaultConf.get(ALLOWED_HOSTS_CONF)));
    Assert.assertFalse(OAuthUrlValidator.AllowedHosts.parse(defaultConf.get(ALLOWED_HOSTS_CONF)).isRestricted());
    Assert.assertNull(defaultConf.get("security.auth.oauth.allow.private.urls"));
  }

  @Test
  public void testCreateProvider() throws IOException {
    String loginURL = "https://" + TEST_DOMAIN + "/login";
    String tokenRefreshURL = "https://" + TEST_DOMAIN + "/token";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret");
    HttpResponse createResponse = makePutCall("provider/testprovider", request);
    Assert.assertEquals(200, createResponse.getResponseCode());

    HttpResponse getResponse = makeGetCall("provider/testprovider/authurl");
    Assert.assertEquals(200, getResponse.getResponseCode());
    String authURL = getResponse.getResponseBodyAsString();
    Assert.assertEquals("https://" + TEST_DOMAIN + "/login?client_id=clientid&redirect_uri=null", authURL);
  }

  @Test
  public void testCreateProviderWithClientCredentialsMissing() throws IOException {
    String loginURL = "https://" + TEST_DOMAIN + "/login";
    String tokenRefreshURL = "https://" + TEST_DOMAIN + "/token";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, null, null);
    HttpResponse createResponse = makePutCall("provider/testprovider", request);
    Assert.assertEquals(400, createResponse.getResponseCode());
  }

  @Test
  public void testCreateProviderWithReuseClientCredentialsTrue() throws IOException {
    String loginURL = "https://" + TEST_DOMAIN + "/login";
    String tokenRefreshURL = "https://" + TEST_DOMAIN + "/token";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, null, null);
    HttpResponse createResponse = makePutCall("provider/testprovider10?reuse_client_credentials=true", request);
    Assert.assertEquals(500, createResponse.getResponseCode());
  }

  @Test
  public void testCreateProviderReuseCredentialsWithReuseClientCredentialsTrue() throws IOException {
    String loginURL = "https://" + TEST_DOMAIN + "/login20";
    String tokenRefreshURL = "https://" + TEST_DOMAIN + "/token20";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret");
    HttpResponse createResponse = makePutCall("provider/testprovider20", request);
    Assert.assertEquals(200, createResponse.getResponseCode());

    loginURL = "https://" + TEST_DOMAIN + "/login21";
    tokenRefreshURL = "https://" + TEST_DOMAIN + "/token21";
    request = createPutRequest(loginURL, tokenRefreshURL, null, null);
    createResponse = makePutCall("provider/testprovider20?reuse_client_credentials=true", request);
    Assert.assertEquals(200, createResponse.getResponseCode());
  }

  @Test
  public void testCreateProviderWithReuseClientCredentialsFalse() throws IOException {
    String loginURL = "https://" + TEST_DOMAIN + "/login30";
    String tokenRefreshURL = "https://" + TEST_DOMAIN + "/token30";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, null, null);
    HttpResponse createResponse = makePutCall("provider/testprovider30?reuse_client_credentials=false", request);
    Assert.assertEquals(400, createResponse.getResponseCode());
  }

  @Test
  public void testCreateProviderWithBasicAuth() throws IOException {
    String loginURL = "https://" + TEST_DOMAIN + "/login31";
    String tokenRefreshURL = "https://" + TEST_DOMAIN + "/token31";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret",
        OAuthProvider.CredentialEncodingStrategy.BASIC_AUTH, null, null);
    HttpResponse createOauthProviderResponse = makePutCall("provider/testprovider31", request);
    Assert.assertEquals(200, createOauthProviderResponse.getResponseCode());

    HttpResponse getAuthUrlResponse = makeGetCall("provider/testprovider31/authurl");
    Assert.assertEquals(200, getAuthUrlResponse.getResponseCode());
    String authURL = getAuthUrlResponse.getResponseBodyAsString();
    Assert.assertEquals("https://" + TEST_DOMAIN + "/login31?client_id=clientid&redirect_uri=null", authURL);
  }

  @Test
  public void testCreateProviderWithBasicAuthAndUserAgent() throws IOException {
    String loginURL = "https://" + TEST_DOMAIN + "/login32";
    String tokenRefreshURL = "https://" + TEST_DOMAIN + "/token32";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret",
        OAuthProvider.CredentialEncodingStrategy.BASIC_AUTH, "cdap-test", null);
    HttpResponse createOauthProviderResponse = makePutCall("provider/testprovider32", request);
    Assert.assertEquals(200, createOauthProviderResponse.getResponseCode());

    HttpResponse getAuthUrlResponse = makeGetCall("provider/testprovider32/authurl");
    Assert.assertEquals(200, getAuthUrlResponse.getResponseCode());
    String authURL = getAuthUrlResponse.getResponseBodyAsString();
    Assert.assertEquals("https://" + TEST_DOMAIN + "/login32?client_id=clientid&redirect_uri=null", authURL);
  }

  @Test
  public void testCreateProviderWithPkceAuthType() throws IOException {
    String loginURL = "https://" + TEST_DOMAIN + "/login_pkce";
    String tokenRefreshURL = "https://" + TEST_DOMAIN + "/token_pkce";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret",
        OAuthProvider.CredentialEncodingStrategy.FORM_BODY, null, AuthType.PKCE);
    HttpResponse createOauthProviderResponse = makePutCall("provider/testprovider_pkce", request);
    Assert.assertEquals(200, createOauthProviderResponse.getResponseCode());

    HttpResponse getAuthUrlResponse = makeGetCall("provider/testprovider_pkce/authurl");
    Assert.assertEquals(200, getAuthUrlResponse.getResponseCode());
    String authURL = getAuthUrlResponse.getResponseBodyAsString();

    Assert.assertTrue(authURL.startsWith(
        "https://" + TEST_DOMAIN + "/login_pkce?client_id=clientid&redirect_uri=null"));
    Assert.assertTrue(authURL.contains("&state="));
    Assert.assertTrue(authURL.contains("&code_challenge="));
    Assert.assertTrue(authURL.contains("&code_challenge_method=S256"));
  }

  @Test
  public void testGetAuthURLForMissingClientCredentials() throws IOException {
    String loginURL = "https://" + TEST_DOMAIN + "/login40";
    String tokenRefreshURL = "https://" + TEST_DOMAIN + "/token40";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, null, null);
    HttpResponse createResponse = makePutCall("provider/testprovider40?reuse_client_credentials=false", request);
    Assert.assertEquals(400, createResponse.getResponseCode());

    HttpResponse getResponse = makeGetCall("provider/testprovider40/authurl");
    Assert.assertEquals(404, getResponse.getResponseCode());
  }

  @Test
  public void testGetAuthURLForReusedClientCredentials() throws IOException {
    String loginURL = "https://" + TEST_DOMAIN + "/login50";
    String tokenRefreshURL = "https://" + TEST_DOMAIN + "/token50";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret");
    HttpResponse createResponse = makePutCall("provider/testprovider50", request);
    Assert.assertEquals(200, createResponse.getResponseCode());

    HttpResponse getResponse = makeGetCall("provider/testprovider50/authurl");
    Assert.assertEquals(200, getResponse.getResponseCode());
    String authURL = getResponse.getResponseBodyAsString();
    Assert.assertEquals("https://" + TEST_DOMAIN + "/login50?client_id=clientid&redirect_uri=null", authURL);

    loginURL = "https://" + TEST_DOMAIN + "/login51";
    tokenRefreshURL = "https://" + TEST_DOMAIN + "/token51";
    request = createPutRequest(loginURL, tokenRefreshURL, null, null);
    createResponse = makePutCall("provider/testprovider50?reuse_client_credentials=true", request);
    Assert.assertEquals(200, createResponse.getResponseCode());

    getResponse = makeGetCall("provider/testprovider50/authurl");
    Assert.assertEquals(200, getResponse.getResponseCode());
    authURL = getResponse.getResponseBodyAsString();
    Assert.assertEquals("https://" + TEST_DOMAIN + "/login51?client_id=clientid&redirect_uri=null", authURL);
  }

  @Test
  public void testCreateProviderBadLoginURL() throws IOException {
    String loginURL = "badurl";
    String tokenRefreshURL = "https://" + TEST_DOMAIN + "/token";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret");
    HttpResponse createResponse = makePutCall("provider/testprovider", request);
    Assert.assertEquals(400, createResponse.getResponseCode());
  }

  @Test
  public void testCreateProviderBadTokenRefreshURL() throws IOException {
    String loginURL = "https://" + TEST_DOMAIN + "/token";
    String tokenRefreshURL = "badurl";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret");
    HttpResponse createResponse = makePutCall("provider/testprovider", request);
    Assert.assertEquals(400, createResponse.getResponseCode());
  }

  @Test
  public void testStrictProductionHandlerRejectsDisallowedUrls() throws IOException {
    String validHttps = "https://" + TEST_DOMAIN + "/ok";
    String[] disallowedUrls = {
        "http://" + TEST_DOMAIN + "/token",
        "file:///etc/passwd",
        "https://user:pass@" + TEST_DOMAIN + "/token",
        "https://localhost:8080/token",
        "https://127.0.0.1/token",
        "https://0.0.0.1/token",
        "https://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token",
        "https://10.0.0.1/token",
        "https://172.16.0.1/token",
        "https://192.168.1.1/token",
        "https://100.64.0.1/token",
        "https://198.18.0.1/token",
        "https://[::1]/token",
        "https://[fe80::1]/token",
        "https://[fc00::1]/token",
        "https://[::ffff:169.254.169.254]/token",
        "https://[64:ff9b::169.254.169.254]/token"
    };

    for (String badUrl : disallowedUrls) {
      HttpResponse respRefresh = makePutCall("provider/ssrf_refresh",
          createPutRequest(validHttps, badUrl, "clientid", "clientsecret"));
      Assert.assertEquals("Expected 400 for tokenRefreshURL: " + badUrl, 400, respRefresh.getResponseCode());
      Assert.assertTrue("Expected OAuth URL validation error for " + badUrl + ", got: "
                            + respRefresh.getResponseBodyAsString(),
                        respRefresh.getResponseBodyAsString().contains("OAuth URL"));

      HttpResponse respLogin = makePutCall("provider/ssrf_login",
          createPutRequest(badUrl, validHttps, "clientid", "clientsecret"));
      Assert.assertEquals("Expected 400 for loginURL: " + badUrl, 400, respLogin.getResponseCode());
      Assert.assertTrue("Expected OAuth URL validation error for " + badUrl + ", got: "
                            + respLogin.getResponseBodyAsString(),
                        respLogin.getResponseBodyAsString().contains("OAuth URL"));
    }
  }

  @Test
  public void testAllowedHostsRestrictiveConfiguration() throws IOException {
    getConfiguration().set(ALLOWED_HOSTS_CONF, "oauth2.googleapis.invalid,*.salesforce.invalid");
    try {
      // Exact match and wildcard subdomain succeed
      HttpResponse okResp = makePutCall("provider/allowlisted_provider",
          createPutRequest("https://acme.salesforce.invalid/login",
                           "https://oauth2.googleapis.invalid/token", "clientid", "clientsecret"));
      Assert.assertEquals(200, okResp.getResponseCode());
      Assert.assertEquals(200, makeGetCall("provider/allowlisted_provider/authurl").getResponseCode());

      // Disallowed host, bare root domain, and lookalike suffix are rejected with 400
      String[] rejected = {
          "https://evil.example.invalid/token",
          "https://salesforce.invalid/token",
          "https://evilsalesforce.invalid/token"
      };
      for (String badUrl : rejected) {
        HttpResponse resp = makePutCall("provider/blocked_host",
            createPutRequest("https://oauth2.googleapis.invalid/login", badUrl, "clientid", "clientsecret"));
        Assert.assertEquals("Expected 400 for " + badUrl, 400, resp.getResponseCode());
        Assert.assertTrue(resp.getResponseBodyAsString().contains("allowlist"));
      }

      // Tightening allowlist blocks authurl generation for previously registered provider
      getConfiguration().set(ALLOWED_HOSTS_CONF, "oauth2.googleapis.invalid");
      HttpResponse tightenedResp = makeGetCall("provider/allowlisted_provider/authurl");
      Assert.assertEquals(400, tightenedResp.getResponseCode());
      Assert.assertTrue(tightenedResp.getResponseBodyAsString().contains("allowlist"));

      // Misconfigured overly broad allowlist fails closed with 500 without leaking raw config
      getConfiguration().set(ALLOWED_HOSTS_CONF, "*");
      HttpResponse misconfiguredResp = makePutCall("provider/misconfigured",
          createPutRequest("https://oauth2.googleapis.invalid/login",
                           "https://oauth2.googleapis.invalid/token", "clientid", "clientsecret"));
      Assert.assertEquals(500, misconfiguredResp.getResponseCode());
      Assert.assertFalse(misconfiguredResp.getResponseBodyAsString().contains("*"));
    } finally {
      getConfiguration().set(ALLOWED_HOSTS_CONF, "");
    }
  }

  @Test
  public void testGetAuthURLProviderDoesNotExist() throws IOException {
    HttpResponse getResponse = makeGetCall("provider/nonexistantprovider/authurl");
    Assert.assertEquals(404, getResponse.getResponseCode());
  }

  @Test
  public void testGetOAuthCredentialStandardLongLivedToken() throws Exception {
    String providerName = "testGetCredLongLived";
    String credentialName = "cred3";

    PutOAuthProviderRequest request = createPutRequest("https://" + TEST_DOMAIN + "/login",
        "https://" + TEST_DOMAIN + "/token", "clientid", "clientsecret");
    HttpResponse createResp = makePutCall("provider/" + providerName, request);
    Assert.assertEquals(200, createResp.getResponseCode());

    OAuthAccessToken accessToken = OAuthAccessToken.newBuilder()
        .withAccessToken("long_lived_token")
        .withExpiresAt(0L)
        .build();
    getSecureStoreManager().put("system",
        "oauthaccesstoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(accessToken), "Test", Collections.emptyMap());

    HttpResponse response = makeGetCall("provider/" + providerName +
        "/credential/" + credentialName);

    Assert.assertEquals(200, response.getResponseCode());
    String body = response.getResponseBodyAsString();
    Assert.assertTrue(body.contains("long_lived_token"));
  }

  @Test
  public void testGetOAuthCredentialStandardAndForwardsHeadersAndBody() throws Exception {
    String providerName = "testGetCredStd";
    String credentialName = "cred2";

    PutOAuthProviderRequest request = createPutRequest("http://127.0.0.1:" + mockTokenPort + "/login",
        mockTokenUrl, "clientid", "clientsecret", OAuthProvider.CredentialEncodingStrategy.BASIC_AUTH,
        "cdap-unit-test", null, null);
    HttpResponse createResp = makePutCall(loopbackServiceURI, "provider/" + providerName, request);
    Assert.assertEquals(200, createResp.getResponseCode());

    OAuthRefreshToken refreshToken =
        new OAuthRefreshToken("valid_refresh_token", "http://redirect");
    getSecureStoreManager().put("system",
        "oauthrefreshtoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(refreshToken), "Test", Collections.emptyMap());

    HttpResponse response = makeGetCall(loopbackServiceURI, "provider/" + providerName +
        "/credential/" + credentialName);

    Assert.assertEquals(200, response.getResponseCode());
    String body = response.getResponseBodyAsString();
    Assert.assertTrue(body.contains("mock_access_token"));
    Assert.assertNotNull(mockTokenHandler.lastAuthHeader.get());
    Assert.assertTrue(mockTokenHandler.lastAuthHeader.get().startsWith("Basic "));
    Assert.assertEquals("grant_type=refresh_token&refresh_token=valid_refresh_token",
                        mockTokenHandler.lastBody.get());
  }

  @Test
  public void testOutboundRedirectsBlockedEndToEnd() throws Exception {
    String providerName = "testRedirectBlocked";
    String credentialName = "cred1";
    String redirectTokenUrl = "http://127.0.0.1:" + mockTokenPort + "/token-redirect";

    PutOAuthProviderRequest request = createPutRequest("http://127.0.0.1:" + mockTokenPort + "/login",
        redirectTokenUrl, "clientid", "clientsecret");
    Assert.assertEquals(200, makePutCall(loopbackServiceURI, "provider/" + providerName, request).getResponseCode());

    HttpResponse credResp = makePutCall(loopbackServiceURI,
        "provider/" + providerName + "/credential/" + credentialName,
        new PutOAuthCredentialRequest("code123", "https://redirect.example.invalid", null));
    Assert.assertEquals(302, credResp.getResponseCode());
    Assert.assertEquals(0, mockTokenHandler.secretHits.get());
  }

  @Test
  public void testIdentityUrlOriginValidationEndToEnd() throws Exception {
    // 1. Cross-origin / IMDS identityUrl returned by upstream token endpoint is dropped and never called
    String badProvider = "testBadIdentity";
    String badCred = "cred1";
    PutOAuthProviderRequest badReq = createPutRequest("http://127.0.0.1:" + mockTokenPort + "/login",
        "http://127.0.0.1:" + mockTokenPort + "/token-bad-identity", "clientid", "clientsecret",
        OAuthProvider.CredentialEncodingStrategy.FORM_BODY, null, AuthType.STANDARD, RefreshType.RTR);
    Assert.assertEquals(200, makePutCall(loopbackServiceURI, "provider/" + badProvider, badReq).getResponseCode());
    Assert.assertEquals(200, makePutCall(loopbackServiceURI,
        "provider/" + badProvider + "/credential/" + badCred,
        new PutOAuthCredentialRequest("code1", "https://redirect.example.invalid", null)).getResponseCode());

    String storedBadJson = new String(
        getSecureStore().getData("system", "oauthaccesstoken-" + badProvider.toLowerCase() + "-" + badCred),
        StandardCharsets.UTF_8);
    OAuthAccessToken storedBad = GSON.fromJson(storedBadJson, OAuthAccessToken.class);
    Assert.assertNull(storedBad.getIdentityUrl());

    // 2. Same-origin identityUrl is persisted and used to validate cached RTR access tokens
    String goodProvider = "testGoodIdentity";
    String goodCred = "cred1";
    PutOAuthProviderRequest goodReq = createPutRequest("http://127.0.0.1:" + mockTokenPort + "/login",
        "http://127.0.0.1:" + mockTokenPort + "/token-good-identity", "clientid", "clientsecret",
        OAuthProvider.CredentialEncodingStrategy.FORM_BODY, null, AuthType.STANDARD, RefreshType.RTR);
    Assert.assertEquals(200, makePutCall(loopbackServiceURI, "provider/" + goodProvider, goodReq).getResponseCode());
    Assert.assertEquals(200, makePutCall(loopbackServiceURI,
        "provider/" + goodProvider + "/credential/" + goodCred,
        new PutOAuthCredentialRequest("code2", "https://redirect.example.invalid", null)).getResponseCode());

    HttpResponse validResp = makeGetCall(loopbackServiceURI,
        "provider/" + goodProvider + "/credential/" + goodCred + "/valid");
    Assert.assertEquals(200, validResp.getResponseCode());
    Assert.assertTrue(validResp.getResponseBodyAsString().replaceAll("\\s+", "").contains("\"isValid\":true"));
    Assert.assertEquals(1, mockTokenHandler.identityHits.get());
  }

  @Test
  public void testCachedIdentityUrlIsCheckedAgainWhenRead() throws Exception {
    // Covers RTR access tokens without expiry that were cached before this check existed, or before
    // the provider URLs or the allowed hosts changed.
    String provider = "testCachedIdentity";
    String cred = "cred1";
    String accessTokenKey = "oauthaccesstoken-" + provider.toLowerCase() + "-" + cred;
    PutOAuthProviderRequest providerReq = createPutRequest("http://127.0.0.1:" + mockTokenPort + "/login",
        mockTokenUrl, "clientid", "clientsecret",
        OAuthProvider.CredentialEncodingStrategy.FORM_BODY, null, AuthType.STANDARD, RefreshType.RTR);
    Assert.assertEquals(200, makePutCall(loopbackServiceURI, "provider/" + provider, providerReq).getResponseCode());
    getSecureStoreManager().put("system", "oauthrefreshtoken-" + provider.toLowerCase() + "-" + cred,
        GSON.toJson(new OAuthRefreshToken("valid_refresh_token", "http://redirect")), "Test",
        Collections.emptyMap());

    // 1. A cached identityUrl on another origin is not called. The handler refreshes the token instead.
    putCachedAccessToken(accessTokenKey, "stale_cross_origin_token",
        "http://localhost:" + mockTokenPort + "/secret");
    HttpResponse crossOriginResp = makeGetCall(loopbackServiceURI, "provider/" + provider + "/credential/" + cred);
    Assert.assertEquals(200, crossOriginResp.getResponseCode());
    Assert.assertTrue(crossOriginResp.getResponseBodyAsString().contains("mock_access_token"));
    Assert.assertFalse(crossOriginResp.getResponseBodyAsString().contains("stale_cross_origin_token"));
    Assert.assertEquals(0, mockTokenHandler.secretHits.get());

    // 2. A same-origin identityUrl that answers 302 is not followed. The handler refreshes the token instead.
    putCachedAccessToken(accessTokenKey, "stale_redirect_token",
        "http://127.0.0.1:" + mockTokenPort + "/identity-redirect");
    HttpResponse redirectResp = makeGetCall(loopbackServiceURI, "provider/" + provider + "/credential/" + cred);
    Assert.assertEquals(200, redirectResp.getResponseCode());
    Assert.assertTrue(redirectResp.getResponseBodyAsString().contains("mock_access_token"));
    Assert.assertFalse(redirectResp.getResponseBodyAsString().contains("stale_redirect_token"));
    Assert.assertEquals(0, mockTokenHandler.secretHits.get());

    // 3. After the allowed hosts are narrowed, neither the cached identityUrl nor the token endpoint is called.
    putCachedAccessToken(accessTokenKey, "cached_token", "http://127.0.0.1:" + mockTokenPort + "/identity");
    getConfiguration().set(ALLOWED_HOSTS_CONF, "oauth2.googleapis.invalid");
    try {
      HttpResponse blockedResp = makeGetCall(loopbackServiceURI,
          "provider/" + provider + "/credential/" + cred + "/valid");
      Assert.assertEquals(400, blockedResp.getResponseCode());
      Assert.assertTrue(blockedResp.getResponseBodyAsString().contains("allowlist"));
      Assert.assertEquals(0, mockTokenHandler.identityHits.get());
    } finally {
      getConfiguration().set(ALLOWED_HOSTS_CONF, "");
    }
  }

  @Test
  public void testIdentityUrlFailingUrlValidationIsNotStored() throws Exception {
    // The identityUrl matches the loginURL origin but its host is outside the allowed hosts.
    // sanitizeIdentityUrl must drop it instead of storing it or failing the token exchange.
    String provider = "testSplitOriginIdentity";
    String cred = "cred1";
    PutOAuthProviderRequest providerReq = createPutRequest("http://localhost:" + mockTokenPort + "/login",
        "http://127.0.0.1:" + mockTokenPort + "/token-good-identity", "clientid", "clientsecret",
        OAuthProvider.CredentialEncodingStrategy.FORM_BODY, null, AuthType.STANDARD, RefreshType.RTR);
    Assert.assertEquals(200, makePutCall(loopbackServiceURI, "provider/" + provider, providerReq).getResponseCode());
    mockTokenHandler.goodIdentityUrl = "http://localhost:" + mockTokenPort + "/identity";

    getConfiguration().set(ALLOWED_HOSTS_CONF, "127.0.0.1");
    try {
      Assert.assertEquals(200, makePutCall(loopbackServiceURI, "provider/" + provider + "/credential/" + cred,
          new PutOAuthCredentialRequest("code3", "https://redirect.example.invalid", null)).getResponseCode());
    } finally {
      getConfiguration().set(ALLOWED_HOSTS_CONF, "");
    }

    String storedJson = new String(
        getSecureStore().getData("system", "oauthaccesstoken-" + provider.toLowerCase() + "-" + cred),
        StandardCharsets.UTF_8);
    OAuthAccessToken stored = GSON.fromJson(storedJson, OAuthAccessToken.class);
    Assert.assertEquals("tok_good_id", stored.getAccessToken());
    Assert.assertNull(stored.getIdentityUrl());
  }

  private void putCachedAccessToken(String key, String accessToken, String identityUrl) throws Exception {
    OAuthAccessToken token = OAuthAccessToken.newBuilder()
        .withAccessToken(accessToken)
        .withExpiresAt(0L)
        .withIdentityUrl(identityUrl)
        .build();
    getSecureStoreManager().put("system", key, GSON.toJson(token), "Cached token", Collections.emptyMap());
  }

  @Test
  public void testGetOAuthCredentialValidityTokenRefresh() throws Exception {
    String providerName = "testRefresh";
    String credentialName = "cred1";

    PutOAuthProviderRequest request = createPutRequest("http://127.0.0.1:" + mockTokenPort + "/login",
        mockTokenUrl, "clientid", "clientsecret");
    HttpResponse createResp = makePutCall(loopbackServiceURI, "provider/" + providerName, request);
    Assert.assertEquals("Create provider failed: " + createResp.getResponseBodyAsString(),
        200, createResp.getResponseCode());

    long expiredTime = System.currentTimeMillis() - 1000000L;
    OAuthAccessToken expiredToken = OAuthAccessToken.newBuilder()
        .withAccessToken("expired_token")
        .withExpiresAt(expiredTime)
        .build();

    getSecureStoreManager().put("system",
        "oauthaccesstoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(expiredToken), "RTR Test", Collections.emptyMap());

    OAuthRefreshToken refreshToken =
        new OAuthRefreshToken("valid_refresh_token", "http://redirect");
    getSecureStoreManager().put("system",
        "oauthrefreshtoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(refreshToken), "RTR Test", Collections.emptyMap());

    HttpResponse response = makeGetCall(loopbackServiceURI, "provider/" + providerName +
        "/credential/" + credentialName + "/valid");

    Assert.assertEquals(200, response.getResponseCode());
    String body = response.getResponseBodyAsString();
    Assert.assertTrue(body.replaceAll("\\s+", "").contains("\"isValid\":true"));
  }

  @Test
  public void testDeleteProvider() throws IOException {
    String loginURL = "https://" + TEST_DOMAIN + "/login_del";
    String tokenRefreshURL = "https://" + TEST_DOMAIN + "/token_del";
    PutOAuthProviderRequest request = createPutRequest(loginURL, tokenRefreshURL, "clientid", "clientsecret");
    HttpResponse createResponse = makePutCall("provider/testprovider_del", request);
    Assert.assertEquals(200, createResponse.getResponseCode());

    HttpResponse deleteResponse = makeDeleteCall("provider/testprovider_del");
    Assert.assertEquals(200, deleteResponse.getResponseCode());

    HttpResponse getResponse = makeGetCall("provider/testprovider_del/authurl");
    Assert.assertEquals(404, getResponse.getResponseCode());
  }

  @Test
  public void testGetOAuthCredentialRTR() throws Exception {
    String providerName = "testGetCredRtr";
    String credentialName = "cred1";

    PutOAuthProviderRequest request = createPutRequest("http://127.0.0.1:" + mockTokenPort + "/login",
        mockTokenUrl, "clientid", "clientsecret", OAuthProvider.CredentialEncodingStrategy.FORM_BODY,
        null, AuthType.STANDARD, RefreshType.RTR);
    HttpResponse createResp = makePutCall(loopbackServiceURI, "provider/" + providerName, request);
    Assert.assertEquals("Create provider failed: " + createResp.getResponseBodyAsString(),
        200, createResp.getResponseCode());

    long expiredTime = System.currentTimeMillis() - 1000000L;
    OAuthAccessToken expiredToken = OAuthAccessToken.newBuilder()
        .withAccessToken("expired_token")
        .withExpiresAt(expiredTime)
        .build();
    getSecureStoreManager().put("system",
        "oauthaccesstoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(expiredToken), "RTR Test", Collections.emptyMap());

    OAuthRefreshToken refreshToken = new OAuthRefreshToken("valid_refresh_token", "http://redirect");
    getSecureStoreManager().put("system",
        "oauthrefreshtoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(refreshToken), "Test", Collections.emptyMap());

    HttpResponse response = makeGetCall(loopbackServiceURI, "provider/" + providerName +
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
    String invalidTokenUrl = "http://127.0.0.1:" + mockTokenPort + "/token-invalid";

    PutOAuthProviderRequest request = createPutRequest("http://127.0.0.1:" + mockTokenPort + "/login",
        invalidTokenUrl, "clientid", "clientsecret");
    HttpResponse createResp = makePutCall(loopbackServiceURI, "provider/" + providerName, request);
    Assert.assertEquals("Create provider failed: " + createResp.getResponseBodyAsString(),
        200, createResp.getResponseCode());

    OAuthRefreshToken refreshToken = new OAuthRefreshToken("revoked_refresh_token", "http://redirect");
    getSecureStoreManager().put("system",
        "oauthrefreshtoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(refreshToken), "Standard Invalid Test", Collections.emptyMap());

    HttpResponse response = makeGetCall(loopbackServiceURI, "provider/" + providerName +
        "/credential/" + credentialName + "/valid");

    Assert.assertEquals(200, response.getResponseCode());
    String body = response.getResponseBodyAsString();
    Assert.assertTrue(body.replaceAll("\\s+", "").contains("\"isValid\":false"));
  }

  @Test
  public void testGetOAuthCredentialValidityInvalidTokenRTR() throws Exception {
    String providerName = "testInvalidRtr";
    String credentialName = "cred1";
    String invalidTokenUrl = "http://127.0.0.1:" + mockTokenPort + "/token-invalid";

    PutOAuthProviderRequest request = createPutRequest("http://127.0.0.1:" + mockTokenPort + "/login",
        invalidTokenUrl, "clientid", "clientsecret", OAuthProvider.CredentialEncodingStrategy.FORM_BODY,
        null, AuthType.STANDARD, RefreshType.RTR);
    HttpResponse createResp = makePutCall(loopbackServiceURI, "provider/" + providerName, request);
    Assert.assertEquals("Create provider failed: " + createResp.getResponseBodyAsString(),
        200, createResp.getResponseCode());

    OAuthRefreshToken refreshToken = new OAuthRefreshToken("revoked_refresh_token", "http://redirect");
    getSecureStoreManager().put("system",
        "oauthrefreshtoken-" + providerName.toLowerCase() + "-" + credentialName.toLowerCase(),
        GSON.toJson(refreshToken), "RTR Invalid Test", Collections.emptyMap());

    HttpResponse response = makeGetCall(loopbackServiceURI, "provider/" + providerName +
        "/credential/" + credentialName + "/valid");

    Assert.assertEquals(200, response.getResponseCode());
    String body = response.getResponseBodyAsString();
    Assert.assertTrue(body.replaceAll("\\s+", "").contains("\"isValid\":false"));
  }

  @Test
  public void testEndpointsRequireAuthorization() throws Exception {
    String provider = "provider/authProvider";
    String credential = provider + "/credential/cred1";
    String loginUrl = "https://" + TEST_DOMAIN + "/login";
    String tokenUrl = "https://" + TEST_DOMAIN + "/token";
    Assert.assertEquals(HttpURLConnection.HTTP_OK, makePutCall(provider,
        createPutRequest(loginUrl, tokenUrl, "clientid", "clientsecret")).getResponseCode());

    user = "alice";
    int[] codes = {
        makeGetCall(provider + "/authurl").getResponseCode(),
        makePutCall(provider + "?reuse_client_credentials=true",
            createPutRequest("https://attacker.example.invalid/login", tokenUrl, null, null)).getResponseCode(),
        makeDeleteCall(provider + "?preserve_client_credentials=true").getResponseCode(),
        makePutCall(credential, new PutOAuthCredentialRequest("mock_code", "https://redirect.example.invalid", null))
            .getResponseCode(),
        makeGetCall(credential).getResponseCode(),
        makeGetCall(credential + "/valid").getResponseCode(),
    };
    int[] expected = new int[codes.length];
    Arrays.fill(expected, HttpURLConnection.HTTP_FORBIDDEN);
    Assert.assertArrayEquals(expected, codes);

    user = null;
    HttpResponse authUrl = makeGetCall(provider + "/authurl");
    Assert.assertEquals(HttpURLConnection.HTTP_OK, authUrl.getResponseCode());
    Assert.assertEquals(loginUrl + "?client_id=clientid&redirect_uri=null", authUrl.getResponseBodyAsString());
  }

  private HttpResponse makeDeleteCall(String endpoint) throws IOException {
    return makeDeleteCall(serviceURI, endpoint);
  }

  private HttpResponse makeDeleteCall(URI baseUri, String endpoint) throws IOException {
    URL url = baseUri
        .resolve(String.format("v1/oauth/%s", endpoint))
        .toURL();
    HttpRequest.Builder builder = HttpRequest.builder(HttpMethod.DELETE, url);
    if (user != null) {
      builder.addHeader(Constants.Security.Headers.USER_ID, user);
    }
    return HttpRequests.execute(builder.build(), new DefaultHttpRequestConfig(false));
  }

  private HttpResponse makeGetCall(String endpoint) throws IOException {
    return makeGetCall(serviceURI, endpoint);
  }

  private HttpResponse makeGetCall(URI baseUri, String endpoint) throws IOException {
    URL url = baseUri
        .resolve(String.format("v1/oauth/%s", endpoint))
        .toURL();
    HttpRequest.Builder builder = HttpRequest.builder(HttpMethod.GET, url);
    if (user != null) {
      builder.addHeader(Constants.Security.Headers.USER_ID, user);
    }
    return HttpRequests.execute(builder.build(), new DefaultHttpRequestConfig(false));
  }

  private <T> HttpResponse makePutCall(String endpoint, T body) throws IOException {
    return makePutCall(serviceURI, endpoint, body);
  }

  private <T> HttpResponse makePutCall(URI baseUri, String endpoint, T body) throws IOException {
    URL url = baseUri
        .resolve(String.format("v1/oauth/%s", endpoint))
        .toURL();
    HttpRequest.Builder builder = HttpRequest.builder(HttpMethod.PUT, url)
        .withBody(GSON.toJson(body));
    if (user != null) {
      builder.addHeader(Constants.Security.Headers.USER_ID, user);
    }
    return HttpRequests.execute(builder.build(), new DefaultHttpRequestConfig(false));
  }
}
