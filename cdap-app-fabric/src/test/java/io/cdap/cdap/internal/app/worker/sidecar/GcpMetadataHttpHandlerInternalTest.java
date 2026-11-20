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
import io.cdap.cdap.common.encryption.NoOpAeadCipher;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.internal.remote.NoOpRemoteAuthenticator;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.proto.credential.NamespaceCredentialProvider;
import io.cdap.cdap.proto.credential.NotFoundException;
import io.cdap.cdap.proto.credential.ProvisionedCredential;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.GcpMetadataTaskContext;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Tests for {@link GcpMetadataHttpHandlerInternal}.
 */
public class GcpMetadataHttpHandlerInternalTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final Gson GSON =
      ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();

  private NettyHttpService httpService;

  @After
  public void tearDown() throws Exception {
    if (httpService != null) {
      httpService.stop();
      httpService = null;
    }
  }

  private String startMetadataService(CConfiguration cConf,
      RemoteAuthenticator remoteAuthenticator,
      NamespaceCredentialProvider credentialProvider) throws Exception {
    httpService = new CommonNettyHttpServiceBuilder(cConf, "test-metadata",
        new NoOpMetricsCollectionService(), true, auditLogContexts -> {}, new NoOpAeadCipher())
        .setHttpHandlers(
            new GcpMetadataHttpHandlerInternal(cConf, remoteAuthenticator, credentialProvider))
        .setChannelPipelineModifier(new ChannelPipelineModifier() {
          @Override
          public void modify(ChannelPipeline pipeline) {
            pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
          }
        })
        .build();
    httpService.start();
    return String.format("http://%s:%s",
        httpService.getBindAddress().getHostName(), httpService.getBindAddress().getPort());
  }

  private CConfiguration createBaseConf(boolean strictMode) throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), true);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.getFeatureFlagString(), strictMode);
    return cConf;
  }

  private static String extractUnlockToken(HttpResponse response) {
    return response.getHeaders().entries().stream()
        .filter(e -> Constants.Security.Headers.METADATA_UNLOCK_TOKEN.equalsIgnoreCase(e.getKey()))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse(null);
  }

  @Test
  public void testStatus() throws Exception {
    NamespaceCredentialProvider provider = Mockito.mock(NamespaceCredentialProvider.class);
    String endpoint = startMetadataService(
        createBaseConf(true), new NoOpRemoteAuthenticator(), provider);

    URL url = new URL(endpoint + "/");
    HttpResponse httpResponse = HttpRequests.execute(HttpRequest.get(url).build());
    Assert.assertEquals(403, httpResponse.getResponseCode());

    HttpRequest httpRequest = HttpRequest.get(url).addHeader(
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE).build();
    httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(200, httpResponse.getResponseCode());
  }

  @Test
  public void testContextLockStateTransitionsAndOneTimeUnlockToken() throws Exception {
    NamespaceCredentialProvider provider = Mockito.mock(NamespaceCredentialProvider.class);
    Mockito.when(provider.provision(Mockito.eq("ns_secure"), Mockito.any()))
        .thenReturn(new ProvisionedCredential("nsa-token-secure",
            Instant.now().plusSeconds(3600)));

    String endpoint = startMetadataService(
        createBaseConf(true), new NoOpRemoteAuthenticator(), provider);

    // 1. IDLE: token endpoint returns dummy "invalidToken" for DFSLocationModule init.
    URL tokenUrl = new URL(
        endpoint + "/computeMetadata/v1/instance/service-accounts/default/token");
    HttpResponse idleTokenResp = HttpRequests.execute(HttpRequest.get(tokenUrl)
        .addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
            GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE)
        .build());
    Assert.assertEquals(200, idleTokenResp.getResponseCode());
    GcpTokenResponse idleToken =
        GSON.fromJson(idleTokenResp.getResponseBodyAsString(), GcpTokenResponse.class);
    Assert.assertEquals("invalidToken", idleToken.accessToken);

    // 2. IDLE -> LOCKED: PUT /set-context returns 200 and a 64-char hex unlock token.
    GcpMetadataTaskContext secureCtx =
        new GcpMetadataTaskContext("ns_secure", "alice", "127.0.0.1", null);
    URL setUrl = new URL(endpoint + "/set-context");
    HttpResponse setResp = HttpRequests.execute(
        HttpRequest.put(setUrl).withBody(GSON.toJson(secureCtx)).build());
    Assert.assertEquals(200, setResp.getResponseCode());
    Assert.assertEquals(
        "Context was set successfully with namespace 'ns_secure'.",
        setResp.getResponseBodyAsString());
    String unlockToken = extractUnlockToken(setResp);
    Assert.assertNotNull(unlockToken);
    Assert.assertTrue(unlockToken.matches("^[0-9a-f]{64}$"));

    // 3. LOCKED: competing PUT /set-context to another namespace is rejected with 403.
    GcpMetadataTaskContext hijackCtx =
        new GcpMetadataTaskContext("ns_victim", "mallory", "127.0.0.1", null);
    HttpResponse hijackSetResp = HttpRequests.execute(
        HttpRequest.put(setUrl).withBody(GSON.toJson(hijackCtx)).build());
    Assert.assertEquals(403, hijackSetResp.getResponseCode());

    // 4. LOCKED: DELETE /clear-context with missing or wrong token is rejected with 403.
    URL clearUrl = new URL(endpoint + "/clear-context");
    HttpResponse missingTokenClearResp = HttpRequests.execute(HttpRequest.delete(clearUrl).build());
    Assert.assertEquals(403, missingTokenClearResp.getResponseCode());

    HttpResponse wrongTokenClearResp = HttpRequests.execute(
        HttpRequest.delete(clearUrl)
            .addHeader(Constants.Security.Headers.METADATA_UNLOCK_TOKEN,
                "0000000000000000000000000000000000000000000000000000000000000000")
            .build());
    Assert.assertEquals(403, wrongTokenClearResp.getResponseCode());

    // Context must still be locked to ns_secure and serve ns_secure's NSA token.
    HttpResponse lockedTokenResp = HttpRequests.execute(HttpRequest.get(tokenUrl)
        .addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
            GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE)
        .build());
    Assert.assertEquals(200, lockedTokenResp.getResponseCode());
    GcpTokenResponse nsaToken =
        GSON.fromJson(lockedTokenResp.getResponseBodyAsString(), GcpTokenResponse.class);
    Assert.assertEquals("nsa-token-secure", nsaToken.accessToken);

    // 5. LOCKED -> IDLE: DELETE /clear-context with matching unlock token succeeds (200) and allows re-lock.
    HttpResponse validClearResp = HttpRequests.execute(
        HttpRequest.delete(clearUrl)
            .addHeader(Constants.Security.Headers.METADATA_UNLOCK_TOKEN, unlockToken)
            .build());
    Assert.assertEquals(200, validClearResp.getResponseCode());

    NamespaceId secondNs = new NamespaceId("ns_second");
    GcpMetadataTaskContext secondCtx =
        new GcpMetadataTaskContext(secondNs.getNamespace(), "bob", "127.0.0.1", null);
    HttpResponse relockResp = HttpRequests.execute(
        HttpRequest.put(setUrl).withBody(GSON.toJson(secondCtx)).build());
    Assert.assertEquals(200, relockResp.getResponseCode());
    String secondUnlockToken = extractUnlockToken(relockResp);
    Assert.assertNotNull(secondUnlockToken);
    Assert.assertNotEquals(unlockToken, secondUnlockToken);

    HttpResponse finalClearResp = HttpRequests.execute(
        HttpRequest.delete(clearUrl)
            .addHeader(Constants.Security.Headers.METADATA_UNLOCK_TOKEN, secondUnlockToken)
            .build());
    Assert.assertEquals(200, finalClearResp.getResponseCode());
  }

  @Test
  public void testStrictModeFailsClosedOnNotFoundAndUnauthorizedWithoutFallback()
      throws Exception {
    AtomicInteger remoteAuthCalls = new AtomicInteger(0);
    RemoteAuthenticator forbiddenFallbackAuthenticator = new RemoteAuthenticator() {
      @Override
      public String getName() {
        return "spy-remote-auth";
      }

      @Override
      public Credential getCredentials() {
        remoteAuthCalls.incrementAndGet();
        return new Credential("LEAKED_P4SA_TOKEN", Credential.CredentialType.EXTERNAL, 3600L);
      }

      @Override
      public Credential getCredentials(String scopes) {
        return getCredentials();
      }
    };

    NamespaceCredentialProvider provider = Mockito.mock(NamespaceCredentialProvider.class);
    Mockito.when(provider.provision(Mockito.eq("ns_unbound"), Mockito.any()))
        .thenThrow(new NotFoundException("Credential Identity not found for namespace 'ns_unbound'."));
    Mockito.when(provider.provision(Mockito.eq("ns_unauthorized"), Mockito.any()))
        .thenThrow(new UnauthorizedException("Principal lacks PROVISION_CREDENTIAL"));
    Mockito.when(provider.provision(Mockito.eq("ns_secure"), Mockito.any()))
        .thenReturn(new ProvisionedCredential("nsa-token-ok",
            Instant.now().plusSeconds(3600)));

    String endpoint = startMetadataService(
        createBaseConf(true), forbiddenFallbackAuthenticator, provider);
    URL setUrl = new URL(endpoint + "/set-context");
    URL clearUrl = new URL(endpoint + "/clear-context");
    URL tokenUrl = new URL(
        endpoint + "/computeMetadata/v1/instance/service-accounts/default/token");

    // 1. Unbound namespace returns 403 Forbidden and never calls RemoteAuthenticator fallback.
    HttpResponse setUnbound = HttpRequests.execute(HttpRequest.put(setUrl)
        .withBody(GSON.toJson(new GcpMetadataTaskContext("ns_unbound", "alice", "127.0.0.1", null)))
        .build());
    String unboundUnlock = extractUnlockToken(setUnbound);
    HttpResponse unboundTokenResp = HttpRequests.execute(HttpRequest.get(tokenUrl)
        .addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
            GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE)
        .build());
    Assert.assertEquals(403, unboundTokenResp.getResponseCode());
    Assert.assertTrue(
        unboundTokenResp.getResponseBodyAsString().contains("No Namespace Service Account"));
    Assert.assertEquals(0, remoteAuthCalls.get());
    Assert.assertEquals(200, HttpRequests.execute(HttpRequest.delete(clearUrl)
        .addHeader(Constants.Security.Headers.METADATA_UNLOCK_TOKEN, unboundUnlock)
        .build()).getResponseCode());

    // 2. Singleton fallback state was NOT mutated: subsequent NSA namespace still provisions 200 OK.
    HttpResponse setSecure = HttpRequests.execute(HttpRequest.put(setUrl)
        .withBody(GSON.toJson(new GcpMetadataTaskContext("ns_secure", "alice", "127.0.0.1", null)))
        .build());
    String secureUnlock = extractUnlockToken(setSecure);
    HttpResponse secureTokenResp = HttpRequests.execute(HttpRequest.get(tokenUrl)
        .addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
            GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE)
        .build());
    Assert.assertEquals(200, secureTokenResp.getResponseCode());
    Assert.assertTrue(secureTokenResp.getResponseBodyAsString().contains("nsa-token-ok"));
    Assert.assertEquals(200, HttpRequests.execute(HttpRequest.delete(clearUrl)
        .addHeader(Constants.Security.Headers.METADATA_UNLOCK_TOKEN, secureUnlock)
        .build()).getResponseCode());

    // 3. UnauthorizedException is unwrapped cleanly to 403 Forbidden (not 500).
    HttpResponse setUnauth = HttpRequests.execute(HttpRequest.put(setUrl)
        .withBody(GSON.toJson(
            new GcpMetadataTaskContext("ns_unauthorized", "eve", "127.0.0.1", null)))
        .build());
    String unauthUnlock = extractUnlockToken(setUnauth);
    HttpResponse unauthTokenResp = HttpRequests.execute(HttpRequest.get(tokenUrl)
        .addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
            GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE)
        .build());
    Assert.assertEquals(403, unauthTokenResp.getResponseCode());
    Assert.assertEquals(0, remoteAuthCalls.get());
    Assert.assertEquals(200, HttpRequests.execute(HttpRequest.delete(clearUrl)
        .addHeader(Constants.Security.Headers.METADATA_UNLOCK_TOKEN, unauthUnlock)
        .build()).getResponseCode());
  }

  @Test
  public void testNonStrictModeFallsBackToRemoteAuthenticatorAndResetsOnClear() throws Exception {
    AtomicInteger remoteAuthCalls = new AtomicInteger(0);
    RemoteAuthenticator legacyFallbackAuthenticator = new RemoteAuthenticator() {
      @Override
      public String getName() {
        return "legacy-remote-auth";
      }

      @Override
      public Credential getCredentials() {
        remoteAuthCalls.incrementAndGet();
        return new Credential("legacy-fallback-token", Credential.CredentialType.EXTERNAL, 3599L);
      }

      @Override
      public Credential getCredentials(String scopes) {
        return getCredentials();
      }
    };

    NamespaceCredentialProvider provider = Mockito.mock(NamespaceCredentialProvider.class);
    Mockito.when(provider.provision(Mockito.eq("ns_legacy"), Mockito.any()))
        .thenThrow(new NotFoundException("Credential Identity not found for namespace 'ns_legacy'."));
    Mockito.when(provider.provision(Mockito.eq("ns_secure"), Mockito.any()))
        .thenReturn(new ProvisionedCredential("nsa-token-after-clear",
            Instant.now().plusSeconds(3600)));

    String endpoint = startMetadataService(
        createBaseConf(false), legacyFallbackAuthenticator, provider);
    URL setUrl = new URL(endpoint + "/set-context");
    final URL clearUrl = new URL(endpoint + "/clear-context");
    URL tokenUrl = new URL(
        endpoint + "/computeMetadata/v1/instance/service-accounts/default/token");

    // 1. In non-strict mode, NotFoundException falls back to RemoteAuthenticator (200 OK).
    HttpResponse setLegacy = HttpRequests.execute(HttpRequest.put(setUrl)
        .withBody(GSON.toJson(new GcpMetadataTaskContext("ns_legacy", "alice", "127.0.0.1", null)))
        .build());
    final String legacyUnlock = extractUnlockToken(setLegacy);

    HttpResponse fallbackResp = HttpRequests.execute(HttpRequest.get(tokenUrl)
        .addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
            GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE)
        .build());
    Assert.assertEquals(200, fallbackResp.getResponseCode());
    Assert.assertTrue(fallbackResp.getResponseBodyAsString().contains("legacy-fallback-token"));
    Assert.assertEquals(1, remoteAuthCalls.get());

    // Second call within the same task uses credentialIdentityPresent=false fast path.
    HttpResponse secondFallbackResp = HttpRequests.execute(HttpRequest.get(tokenUrl)
        .addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
            GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE)
        .build());
    Assert.assertEquals(200, secondFallbackResp.getResponseCode());
    Assert.assertEquals(2, remoteAuthCalls.get());
    Mockito.verify(provider, Mockito.times(1)).provision(Mockito.eq("ns_legacy"), Mockito.any());

    // 2. Clearing context resets credentialIdentityPresent=true so subsequent NSA task uses NSA.
    Assert.assertEquals(200, HttpRequests.execute(HttpRequest.delete(clearUrl)
        .addHeader(Constants.Security.Headers.METADATA_UNLOCK_TOKEN, legacyUnlock)
        .build()).getResponseCode());

    HttpResponse setSecure = HttpRequests.execute(HttpRequest.put(setUrl)
        .withBody(GSON.toJson(new GcpMetadataTaskContext("ns_secure", "bob", "127.0.0.1", null)))
        .build());
    String secureUnlock = extractUnlockToken(setSecure);
    HttpResponse secureResp = HttpRequests.execute(HttpRequest.get(tokenUrl)
        .addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
            GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE)
        .build());
    Assert.assertEquals(200, secureResp.getResponseCode());
    Assert.assertTrue(secureResp.getResponseBodyAsString().contains("nsa-token-after-clear"));
    Assert.assertEquals(2, remoteAuthCalls.get());
    Assert.assertEquals(200, HttpRequests.execute(HttpRequest.delete(clearUrl)
        .addHeader(Constants.Security.Headers.METADATA_UNLOCK_TOKEN, secureUnlock)
        .build()).getResponseCode());
  }
}
