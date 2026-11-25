/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.encryption.NoOpAeadCipher;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.common.internal.remote.NoOpInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.NoOpRemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.utils.GcpMetadataTaskContextUtil;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.credential.ProvisionedCredential;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.GcpMetadataTaskContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.net.InetAddress;
import java.net.URL;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * Tests for {@link GcpMetadataHttpHandlerInternal}.
 */
public class GcpMetadataHttpHandlerInternalTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final Gson GSON =
      ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  // The token endpoint returns this placeholder while no context is set.
  private static final String NO_CONTEXT_TOKEN = "invalidToken";
  // The fake provision endpoint returns this prefix followed by the namespace of the context.
  private static final String TOKEN_PREFIX = "token-";
  private static final String TOKEN_PATH =
      "/computeMetadata/v1/instance/service-accounts/default/token";
  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";
  private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
  private static NettyHttpService httpService;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration cConf = createCConf();

    NamespaceAdmin namespaceAdmin = new InMemoryNamespaceAdmin();
    namespaceAdmin.create(NamespaceMeta.SYSTEM);
    namespaceAdmin.create(NamespaceMeta.DEFAULT);

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    RemoteClientFactory remoteClientFactory =
        new RemoteClientFactory(discoveryService, new NoOpInternalAuthenticator());
    httpService = new CommonNettyHttpServiceBuilder(cConf, "test",
        new NoOpMetricsCollectionService(), true, auditLogContexts -> {}, new NoOpAeadCipher())
        // GcpMetadataTaskContextUtil connects to the loopback address.
        .setHost(InetAddress.getLoopbackAddress().getHostName())
        .setHttpHandlers(
            new GcpMetadataHttpHandlerInternal(cConf, remoteClientFactory,
                new NoOpRemoteAuthenticator()),
            new FakeProvisionHandler()
        )
        .setChannelPipelineModifier(new ChannelPipelineModifier() {
          @Override
          public void modify(ChannelPipeline pipeline) {
            pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
          }
        })
        .build();
    httpService.start();
    discoveryService.register(
        URIScheme.createDiscoverable(Constants.Service.APP_FABRIC_HTTP, httpService));
  }

  @AfterClass
  public static void finish() throws Exception {
    httpService.stop();
  }

  @After
  public void clearContext() throws Exception {
    // The handler is shared by all tests, so no test may leave a context behind.
    SecurityRequestContext.reset();
    Assert.assertEquals(200, clearContextWithHeader().getResponseCode());
  }

  @Test
  public void testStatus() throws Exception {
    String endpoint = String.format("http://%s:%s/",
        httpService.getBindAddress().getHostName(), httpService.getBindAddress().getPort());
    URL url = new URL(endpoint);
    HttpRequest httpRequest = HttpRequest.get(url).build();
    HttpResponse httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(403, httpResponse.getResponseCode());

    httpRequest = HttpRequest.get(url).addHeader(
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE).build();
    httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(200, httpResponse.getResponseCode());
  }

  @Test
  public void testSetAndClearContext() throws Exception {
    String endpoint = String.format("http://%s:%s",
        httpService.getBindAddress().getHostName(), httpService.getBindAddress().getPort());
    NamespaceId namespaceId = new NamespaceId("test");
    GcpMetadataTaskContext gcpMetadataTaskContext =
        new GcpMetadataTaskContext(namespaceId.getNamespace(),
            "alice", "0.0.0.0", null);

    // set context
    URL url = new URL(String.format("%s/set-context", endpoint));
    HttpRequest httpRequest = HttpRequest.put(url).withBody(GSON.toJson(gcpMetadataTaskContext,
        GcpMetadataTaskContext.class)).addHeader(
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE).build();
    HttpResponse httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(200, httpResponse.getResponseCode());
    String expectedResponse = String.format("Context was set successfully with namespace '%s'.",
        namespaceId.getNamespace());
    Assert.assertEquals(expectedResponse, httpResponse.getResponseBodyAsString());

    //clear context
    url = new URL(String.format("%s/clear-context", endpoint));
    httpRequest = HttpRequest.delete(url).addHeader(
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE).build();
    httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(200, httpResponse.getResponseCode());
  }

  @Test
  public void testSetContextWithoutMetadataFlavorHeaderIsForbidden() throws Exception {
    HttpResponse response = HttpRequests.execute(
        HttpRequest.put(setContextUrl()).withBody(contextJson("test")).build());
    Assert.assertEquals(403, response.getResponseCode());
    Assert.assertEquals(NO_CONTEXT_TOKEN, fetchAccessToken());
  }

  @Test
  public void testSetContextWithWrongMetadataFlavorValueIsForbidden() throws Exception {
    HttpResponse response = HttpRequests.execute(
        HttpRequest.put(setContextUrl()).withBody(contextJson("test"))
            .addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY, "NotGoogle")
            .build());
    Assert.assertEquals(403, response.getResponseCode());
    Assert.assertEquals(NO_CONTEXT_TOKEN, fetchAccessToken());
  }

  @Test
  public void testSetContextAcceptsLowercaseMetadataFlavorValue() throws Exception {
    HttpResponse response = HttpRequests.execute(
        HttpRequest.put(setContextUrl()).withBody(contextJson("test"))
            .addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY, "google")
            .build());
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(TOKEN_PREFIX + "test", fetchAccessToken());
  }

  @Test
  public void testForbiddenSetContextKeepsExistingContext() throws Exception {
    setContextWithHeader("test");

    HttpResponse response = HttpRequests.execute(
        HttpRequest.put(setContextUrl()).withBody(contextJson("other")).build());
    Assert.assertEquals(403, response.getResponseCode());
    Assert.assertEquals(TOKEN_PREFIX + "test", fetchAccessToken());
  }

  @Test
  public void testClearContextWithoutMetadataFlavorHeaderIsForbidden() throws Exception {
    setContextWithHeader("test");

    HttpResponse response =
        HttpRequests.execute(HttpRequest.delete(clearContextUrl()).build());
    Assert.assertEquals(403, response.getResponseCode());
    Assert.assertEquals(TOKEN_PREFIX + "test", fetchAccessToken());
  }

  @Test
  public void testClearContextWithWrongMetadataFlavorValueIsForbidden() throws Exception {
    setContextWithHeader("test");

    HttpResponse response = HttpRequests.execute(
        HttpRequest.delete(clearContextUrl())
            .addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY, "NotGoogle")
            .build());
    Assert.assertEquals(403, response.getResponseCode());
    Assert.assertEquals(TOKEN_PREFIX + "test", fetchAccessToken());
  }

  @Test
  public void testClearContextWithMetadataFlavorHeaderClearsContext() throws Exception {
    setContextWithHeader("test");

    HttpResponse response = clearContextWithHeader();
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(NO_CONTEXT_TOKEN, fetchAccessToken());
  }

  @Test
  public void testTaskContextUtilSetsContext() throws Exception {
    CConfiguration cConf = createTaskContextUtilConf();
    // The util copies the caller's identity into the context it sends to the sidecar.
    SecurityRequestContext.setUserId("alice");
    SecurityRequestContext.setUserCredential(
        new Credential("credential", Credential.CredentialType.EXTERNAL));

    GcpMetadataTaskContextUtil.setGcpMetadataTaskContext(new NamespaceId("test"), cConf);
    Assert.assertEquals(TOKEN_PREFIX + "test", fetchAccessToken());
  }

  @Test
  public void testTaskContextUtilClearsContext() throws Exception {
    CConfiguration cConf = createTaskContextUtilConf();
    setContextWithHeader("test");

    GcpMetadataTaskContextUtil.clearGcpMetadataTaskContext(cConf);
    Assert.assertEquals(NO_CONTEXT_TOKEN, fetchAccessToken());
  }

  @Test
  public void testExpiresInCountsDownWithoutRefetching() throws Exception {
    FakeAppFabric appFabric = new FakeAppFabric();
    appFabric.issue("token-1", START.plus(Duration.ofMinutes(60)));
    MutableClock clock = new MutableClock(START);
    NettyHttpService service = startHandler(appFabric, new NoOpRemoteAuthenticator(), clock);
    try {
      setContext(service);

      assertToken(service, "token-1", 3600);
      clock.advance(Duration.ofMinutes(10));
      assertToken(service, "token-1", 3000);
      Assert.assertEquals(1, appFabric.provisionCount);
    } finally {
      service.stop();
    }
  }

  @Test
  public void testTokenNearExpiryIsRefetched() throws Exception {
    // App Fabric caches tokens too, so it can hand out one with only 12 minutes left.
    FakeAppFabric appFabric = new FakeAppFabric();
    appFabric.issue("token-1", START.plus(Duration.ofMinutes(12)));
    appFabric.issue("token-2", START.plus(Duration.ofMinutes(67)));
    MutableClock clock = new MutableClock(START);
    NettyHttpService service = startHandler(appFabric, new NoOpRemoteAuthenticator(), clock);
    try {
      setContext(service);

      assertToken(service, "token-1", 720);
      clock.advance(Duration.ofMinutes(6));
      assertToken(service, "token-1", 360);
      // Within the refresh buffer of token-1's expiry.
      clock.advance(Duration.ofMinutes(1));
      assertToken(service, "token-2", 3600);
      Assert.assertEquals(2, appFabric.provisionCount);
    } finally {
      service.stop();
    }
  }

  @Test
  public void testNewTokenNearExpiryIsServedWithoutRetrying() throws Exception {
    FakeAppFabric appFabric = new FakeAppFabric();
    appFabric.issue("token-1", START.plus(Duration.ofMinutes(60)));
    appFabric.issue("token-2", START.plus(Duration.ofMinutes(59)));
    MutableClock clock = new MutableClock(START);
    NettyHttpService service = startHandler(appFabric, new NoOpRemoteAuthenticator(), clock);
    try {
      setContext(service);
      assertToken(service, "token-1", 3600);

      // token-1 is now within the refresh buffer. The replacement has even less time left, but it
      // is served as is, with its real lifetime, instead of being fetched again.
      clock.advance(Duration.ofMinutes(56));
      assertToken(service, "token-2", 180);
      Assert.assertEquals(2, appFabric.provisionCount);
    } finally {
      service.stop();
    }
  }

  @Test
  public void testFirstTokenNearExpiryIsFetchedOnce() throws Exception {
    FakeAppFabric appFabric = new FakeAppFabric();
    appFabric.issue("token-1", START.plus(Duration.ofMinutes(4)));
    MutableClock clock = new MutableClock(START);
    NettyHttpService service = startHandler(appFabric, new NoOpRemoteAuthenticator(), clock);
    try {
      setContext(service);

      assertToken(service, "token-1", 240);
      Assert.assertEquals(1, appFabric.provisionCount);
    } finally {
      service.stop();
    }
  }

  @Test
  public void testClearContextForcesRefetch() throws Exception {
    FakeAppFabric appFabric = new FakeAppFabric();
    appFabric.issue("token-1", START.plus(Duration.ofMinutes(60)));
    appFabric.issue("token-2", START.plus(Duration.ofMinutes(60)));
    MutableClock clock = new MutableClock(START);
    NettyHttpService service = startHandler(appFabric, new NoOpRemoteAuthenticator(), clock);
    try {
      setContext(service);
      assertToken(service, "token-1", 3600);

      HttpResponse response = HttpRequests.execute(
          withMetadataFlavor(HttpRequest.delete(new URL(baseUrl(service) + "/clear-context")))
              .build());
      Assert.assertEquals(200, response.getResponseCode());
      setContext(service);

      assertToken(service, "token-2", 3600);
      Assert.assertEquals(2, appFabric.provisionCount);
    } finally {
      service.stop();
    }
  }

  @Test
  public void testFallbackTokenReportsRemainingLifetime() throws Exception {
    // No namespace identity, so the handler falls back to the pod's own credentials, whose
    // expiry is reported in epoch seconds.
    FakeAppFabric appFabric = new FakeAppFabric();
    long expiryEpochSecs = START.plus(Duration.ofMinutes(30)).getEpochSecond();
    RemoteAuthenticator remoteAuthenticator = new FixedRemoteAuthenticator(
        new Credential("pod-token", Credential.CredentialType.EXTERNAL_BEARER, expiryEpochSecs));
    MutableClock clock = new MutableClock(START);
    NettyHttpService service = startHandler(appFabric, remoteAuthenticator, clock);
    try {
      setContext(service);

      assertToken(service, "pod-token", 1800);
      clock.advance(Duration.ofMinutes(10));
      assertToken(service, "pod-token", 1200);
      assertTokenForScopes(service, "pod-token", 1200, CLOUD_PLATFORM_SCOPE);
      clock.advance(Duration.ofMinutes(5));
      assertTokenForScopes(service, "pod-token", 900, CLOUD_PLATFORM_SCOPE);
    } finally {
      service.stop();
    }
  }

  @Test
  public void testFallbackTokenWithoutExpiryFails() throws Exception {
    FakeAppFabric appFabric = new FakeAppFabric();
    RemoteAuthenticator remoteAuthenticator = new FixedRemoteAuthenticator(
        new Credential("pod-token", Credential.CredentialType.EXTERNAL_BEARER));
    NettyHttpService service = startHandler(appFabric, remoteAuthenticator,
        new MutableClock(START));
    try {
      setContext(service);

      for (String scopes : new String[] {null, CLOUD_PLATFORM_SCOPE}) {
        HttpResponse response = getToken(service, scopes);
        Assert.assertEquals(500, response.getResponseCode());
        Assert.assertTrue(response.getResponseBodyAsString(),
            response.getResponseBodyAsString().contains("no expiration time"));
      }
    } finally {
      service.stop();
    }
  }

  @Test
  public void testRefreshedUserContextKeepsCachedTokenAndAuthenticatesNextRefetch()
      throws Exception {
    FakeAppFabric appFabric = new FakeAppFabric();
    appFabric.issue("token-1", START.plus(Duration.ofMinutes(12)));
    appFabric.issue("token-bob", START.plus(Duration.ofMinutes(12)));
    appFabric.issue("token-2", START.plus(Duration.ofMinutes(67)));
    MutableClock clock = new MutableClock(START);
    NettyHttpService service = startHandler(appFabric, new NoOpRemoteAuthenticator(), clock);
    try {
      setContext(service, "alice", "user-token-1");
      assertToken(service, "token-1", 720);
      Assert.assertEquals("Bearer user-token-1",
          appFabric.lastAuthHeaders.get(Constants.Security.Headers.RUNTIME_TOKEN));

      // Same user with a null credential or rotated user token still hits the cache without NPE.
      setContext(service, "alice", null);
      assertToken(service, "token-1", 720);
      setContext(service, "alice", "user-token-2");
      clock.advance(Duration.ofMinutes(2));
      assertToken(service, "token-1", 600);
      Assert.assertEquals(1, appFabric.provisionCount);

      // A different user misses the sidecar cache so App Fabric enforces PROVISION_CREDENTIAL.
      setContext(service, "bob", "bob-token");
      assertToken(service, "token-bob", 600);
      Assert.assertEquals(2, appFabric.provisionCount);
      Assert.assertEquals("Bearer bob-token",
          appFabric.lastAuthHeaders.get(Constants.Security.Headers.RUNTIME_TOKEN));

      // Once alice's token-1 enters the 5m refresh buffer, the sidecar re-provisions with user-token-2.
      setContext(service, "alice", "user-token-2");
      clock.advance(Duration.ofMinutes(5));
      assertToken(service, "token-2", 3600);
      Assert.assertEquals(3, appFabric.provisionCount);
      Assert.assertEquals("Bearer user-token-2",
          appFabric.lastAuthHeaders.get(Constants.Security.Headers.RUNTIME_TOKEN));
    } finally {
      service.stop();
    }
  }

  @Test
  public void testExpiredTokenClampsExpiresInToZero() throws Exception {
    FakeAppFabric appFabric = new FakeAppFabric();
    appFabric.issue("token-expired", START.minus(Duration.ofSeconds(10)));
    MutableClock clock = new MutableClock(START);
    NettyHttpService service = startHandler(appFabric, new NoOpRemoteAuthenticator(), clock);
    try {
      setContext(service);
      assertToken(service, "token-expired", 0);
      Assert.assertEquals(1, appFabric.provisionCount);
    } finally {
      service.stop();
    }
  }

  private static CConfiguration createCConf() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    return cConf;
  }

  private static NettyHttpService startService(CConfiguration cConf,
      GcpMetadataHttpHandlerInternal handler) throws Exception {
    NettyHttpService service = new CommonNettyHttpServiceBuilder(cConf, "test",
        new NoOpMetricsCollectionService(), true, auditLogContexts -> {}, new NoOpAeadCipher())
        .setHttpHandlers(handler)
        .setChannelPipelineModifier(new ChannelPipelineModifier() {
          @Override
          public void modify(ChannelPipeline pipeline) {
            pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
          }
        })
        .build();
    service.start();
    return service;
  }

  private static NettyHttpService startHandler(FakeAppFabric appFabric,
      RemoteAuthenticator remoteAuthenticator, Clock clock) throws Exception {
    CConfiguration cConf = createCConf();
    return startService(cConf, new GcpMetadataHttpHandlerInternal(cConf,
        appFabric.remoteClientFactory(), remoteAuthenticator, clock));
  }

  private static String baseUrl(NettyHttpService service) {
    return String.format("http://%s:%s", service.getBindAddress().getHostName(),
        service.getBindAddress().getPort());
  }

  private static void setContext(NettyHttpService service) throws Exception {
    setContext(service, "alice", "user-token");
  }

  private static void setContext(NettyHttpService service, String userId,
      @Nullable String userToken) throws Exception {
    Credential credential = userToken == null
        ? null : new Credential(userToken, Credential.CredentialType.EXTERNAL_BEARER);
    GcpMetadataTaskContext context =
        new GcpMetadataTaskContext("test", userId, "0.0.0.0", credential);
    HttpResponse response = HttpRequests.execute(
        withMetadataFlavor(HttpRequest.put(new URL(baseUrl(service) + "/set-context")))
            .withBody(GSON.toJson(context, GcpMetadataTaskContext.class)).build());
    Assert.assertEquals(200, response.getResponseCode());
  }

  private static HttpResponse getToken(NettyHttpService service, @Nullable String scopes)
      throws Exception {
    String url = baseUrl(service) + TOKEN_PATH + (scopes == null ? "" : "?scopes=" + scopes);
    return HttpRequests.execute(HttpRequest.get(new URL(url))
        .addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
            GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE)
        .build());
  }

  private static void assertToken(NettyHttpService service, String expectedToken,
      long expectedExpiresInSecs) throws Exception {
    assertTokenForScopes(service, expectedToken, expectedExpiresInSecs, null);
  }

  private static void assertTokenForScopes(NettyHttpService service, String expectedToken,
      long expectedExpiresInSecs, @Nullable String scopes) throws Exception {
    HttpResponse response = getToken(service, scopes);
    Assert.assertEquals(200, response.getResponseCode());
    GcpTokenResponse token =
        GSON.fromJson(response.getResponseBodyAsString(), GcpTokenResponse.class);
    Assert.assertNotNull(token.tokenType);
    Assert.assertFalse(token.tokenType.isEmpty());
    Assert.assertEquals(expectedToken, token.accessToken);
    Assert.assertEquals(expectedExpiresInSecs, token.expiresInSecs);
  }

  /**
   * Stands in for the App Fabric credential provisioning endpoint. Hands out the queued tokens in
   * order, or responds 404 (no namespace identity) when none are queued.
   */
  private static final class FakeAppFabric {
    private final Deque<ProvisionedCredential> tokens = new ArrayDeque<>();
    private final Map<String, String> lastAuthHeaders = new HashMap<>();
    private InternalAuthenticator internalAuthenticator;
    private int provisionCount;

    void issue(String token, Instant expiry) {
      tokens.add(new ProvisionedCredential(token, expiry));
    }

    RemoteClientFactory remoteClientFactory() throws Exception {
      RemoteClient remoteClient = Mockito.mock(RemoteClient.class);
      Mockito.when(remoteClient.requestBuilder(ArgumentMatchers.any(HttpMethod.class),
              ArgumentMatchers.anyString()))
          .thenAnswer(invocation -> HttpRequest.get(
              new URL("http://app-fabric/" + invocation.getArgument(1))));
      Mockito.when(remoteClient.execute(ArgumentMatchers.any(HttpRequest.class),
              ArgumentMatchers.any()))
          .thenAnswer(invocation -> provision());

      RemoteClientFactory remoteClientFactory = Mockito.mock(RemoteClientFactory.class);
      Mockito.when(remoteClientFactory.createRemoteClient(ArgumentMatchers.anyString(),
              ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.any()))
          .thenAnswer(invocation -> {
            internalAuthenticator = invocation.getArgument(3);
            return remoteClient;
          });
      return remoteClientFactory;
    }

    private synchronized HttpResponse provision() {
      lastAuthHeaders.clear();
      if (internalAuthenticator != null) {
        internalAuthenticator.applyInternalAuthenticationHeaders(lastAuthHeaders::put);
      }
      HttpResponse response = Mockito.mock(HttpResponse.class);
      if (tokens.isEmpty()) {
        Mockito.when(response.getResponseCode()).thenReturn(404);
        return response;
      }
      provisionCount++;
      Mockito.when(response.getResponseCode()).thenReturn(200);
      Mockito.when(response.getResponseBodyAsString()).thenReturn(new Gson().toJson(tokens.poll()));
      return response;
    }
  }

  /**
   * A {@link RemoteAuthenticator} that always returns the same credential.
   */
  private static final class FixedRemoteAuthenticator implements RemoteAuthenticator {
    private final Credential credential;

    FixedRemoteAuthenticator(Credential credential) {
      this.credential = credential;
    }

    @Override
    public String getName() {
      return "fixed";
    }

    @Override
    public Credential getCredentials() {
      return credential;
    }

    @Override
    public Credential getCredentials(String scopes) {
      return credential;
    }
  }

  /**
   * A {@link Clock} that only moves when the test advances it.
   */
  private static final class MutableClock extends Clock {
    private volatile Instant now;

    MutableClock(Instant now) {
      this.now = now;
    }

    void advance(Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Instant instant() {
      return now;
    }
  }

  private static CConfiguration createTaskContextUtilConf() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), true);
    cConf.setInt(Constants.ArtifactLocalizer.PORT, httpService.getBindAddress().getPort());
    return cConf;
  }

  private static String baseUrl() {
    return String.format("http://%s:%s",
        httpService.getBindAddress().getHostName(), httpService.getBindAddress().getPort());
  }

  private static URL setContextUrl() throws Exception {
    return new URL(baseUrl() + "/set-context");
  }

  private static URL clearContextUrl() throws Exception {
    return new URL(baseUrl() + "/clear-context");
  }

  private static HttpRequest.Builder withMetadataFlavor(HttpRequest.Builder builder) {
    return builder.addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE);
  }

  private static String contextJson(String namespace) {
    return GSON.toJson(new GcpMetadataTaskContext(namespace, "alice", "0.0.0.0",
        new Credential("credential", Credential.CredentialType.EXTERNAL)),
        GcpMetadataTaskContext.class);
  }

  private static void setContextWithHeader(String namespace) throws Exception {
    HttpResponse response = HttpRequests.execute(
        withMetadataFlavor(HttpRequest.put(setContextUrl())).withBody(contextJson(namespace))
            .build());
    Assert.assertEquals(200, response.getResponseCode());
  }

  private static HttpResponse clearContextWithHeader() throws Exception {
    return HttpRequests.execute(
        withMetadataFlavor(HttpRequest.delete(clearContextUrl())).build());
  }

  /**
   * Returns the access token the sidecar serves: {@link #NO_CONTEXT_TOKEN} while no context is
   * set, otherwise {@link #TOKEN_PREFIX} followed by the namespace of the context.
   */
  private static String fetchAccessToken() throws Exception {
    URL url = new URL(baseUrl()
        + "/computeMetadata/v1/instance/service-accounts/default/token");
    HttpResponse response =
        HttpRequests.execute(withMetadataFlavor(HttpRequest.get(url)).build());
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), GcpTokenResponse.class).accessToken;
  }

  /**
   * Stands in for the App Fabric endpoint that provisions a credential for a namespace.
   */
  @Path(Constants.Gateway.INTERNAL_API_VERSION_3)
  public static final class FakeProvisionHandler extends AbstractHttpHandler {

    /**
     * Returns a token that names the requested namespace.
     */
    @GET
    @Path("/namespaces/{namespace}/credentials/workloadIdentity/provision")
    public void provision(io.netty.handler.codec.http.HttpRequest request,
        HttpResponder responder, @PathParam("namespace") String namespace) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(
          new ProvisionedCredential(TOKEN_PREFIX + namespace, Instant.now().plusSeconds(3600))));
    }
  }
}
