/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.plugin;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.service.ServiceDiscoverable;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeScopes;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TriggerMappedArguments;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ConnectionPropertiesFetcher}.
 */
public class ConnectionPropertiesFetcherTest {

  private static final String DISCOVERY_NAME = ServiceDiscoverable.getName(
      NamespaceId.SYSTEM.getNamespace(), "pipeline", ProgramType.SERVICE, "studio");

  private NettyHttpService httpService;
  private InMemoryDiscoveryService discoveryService;
  private Cancellable registration;
  private StudioStubHandler stubHandler;

  @Before
  public void setUp() throws Exception {
    stubHandler = new StudioStubHandler();
    httpService = NettyHttpService.builder("studio-stub")
        .setHttpHandlers(stubHandler)
        .build();
    httpService.start();
    discoveryService = new InMemoryDiscoveryService();
    registration = discoveryService.register(
        URIScheme.createDiscoverable(DISCOVERY_NAME, httpService));
  }

  @After
  public void tearDown() throws Exception {
    SecurityRequestContext.resetUserDetails();
    if (registration != null) {
      registration.cancel();
    }
    if (httpService != null) {
      httpService.stop();
    }
  }

  @Test
  public void testReadSuccessUrlEncodingAndSystemIdentity() throws Exception {
    AtomicBoolean contextClearedDuringAuth = new AtomicBoolean(false);
    InternalAuthenticator verifyingAuth = headerSetter -> {
      // Must be called with caller's SecurityRequestContext cleared (system identity)
      contextClearedDuringAuth.set(
          SecurityRequestContext.getUserId() == null
              && SecurityRequestContext.getUserIp() == null
              && SecurityRequestContext.getUserCredential() == null);
      headerSetter.accept(Constants.Security.Headers.USER_ID, "system");
    };
    RemoteClientFactory factory = new RemoteClientFactory(discoveryService, verifyingAuth);
    ConnectionPropertiesFetcher fetcher = new ConnectionPropertiesFetcher(factory);

    SecurityRequestContext.setUserId("alice");
    SecurityRequestContext.setUserIp("10.0.0.1");
    Credential callerCred = new Credential("caller-tok", Credential.CredentialType.EXTERNAL);
    SecurityRequestContext.setUserCredential(callerCred);

    RuntimeScopesExtractor.ConnectionReader reader = fetcher.forNamespace("ns1");
    Optional<Map<String, String>> props = reader.read("  my conn+1*  ");
    Assert.assertTrue(props.isPresent());
    Assert.assertEquals(ImmutableMap.of("secret", "${secure(s1)}"), props.get());
    Assert.assertTrue(contextClearedDuringAuth.get());
    // Caller context restored after call
    Assert.assertEquals("alice", SecurityRequestContext.getUserId());
    Assert.assertEquals("10.0.0.1", SecurityRequestContext.getUserIp());
    Assert.assertEquals(callerCred, SecurityRequestContext.getUserCredential());

    // Raw path segment matches URLEncoder.encode("  my conn+1*  ", UTF-8)
    Assert.assertEquals(Collections.singletonList(
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/ns1/connections/"
            + "++my+conn%2B1*++"),
        stubHandler.recordedUris);

    // Responses with null plugin or null plugin.properties return empty properties map
    Optional<Map<String, String>> emptyProps = reader.read("no-plugin");
    Assert.assertTrue(emptyProps.isPresent());
    Assert.assertTrue(emptyProps.get().isEmpty());

    Optional<Map<String, String>> nullProps = reader.read("no-props");
    Assert.assertTrue(nullProps.isPresent());
    Assert.assertTrue(nullProps.get().isEmpty());
  }

  @Test
  public void testNotFoundAndBadRequestReturnEmpty() throws Exception {
    RemoteClientFactory factory = new RemoteClientFactory(discoveryService, setter -> { });
    ConnectionPropertiesFetcher fetcher = new ConnectionPropertiesFetcher(factory);
    RuntimeScopesExtractor.ConnectionReader reader = fetcher.forNamespace("ns1");

    Assert.assertEquals(Optional.empty(), reader.read("not-found"));
    Assert.assertEquals(Optional.empty(), reader.read("bad-request"));
  }

  @Test
  public void testFailuresThrowIoExceptionAndRestoreContext() {
    RemoteClientFactory factory = new RemoteClientFactory(discoveryService, setter -> { });
    ConnectionPropertiesFetcher fetcher = new ConnectionPropertiesFetcher(factory);
    RuntimeScopesExtractor.ConnectionReader reader = fetcher.forNamespace("ns1");

    Credential bobCred = new Credential("bob-tok", Credential.CredentialType.EXTERNAL);
    SecurityRequestContext.setUserId("bob");
    SecurityRequestContext.setUserIp("10.0.0.2");
    SecurityRequestContext.setUserCredential(bobCred);
    // Include "unauthorized" (HTTP 401 with JSON body), which RemoteClient returns as HttpResponse
    // rather than throwing, directly exercising the statusCode != HTTP_OK guard.
    for (String failingName : new String[] {
        "unauthorized", "server-error", "unavailable", "forbidden", "bad-json", "empty-body"
    }) {
      try {
        reader.read(failingName);
        Assert.fail("Expected IOException for " + failingName);
      } catch (IOException expected) {
        // Expected
      }
      Assert.assertEquals("bob", SecurityRequestContext.getUserId());
      Assert.assertEquals("10.0.0.2", SecurityRequestContext.getUserIp());
      Assert.assertEquals(bobCred, SecurityRequestContext.getUserCredential());
    }

    // Undiscoverable service also throws IOException
    RemoteClientFactory emptyDiscoveryFactory =
        new RemoteClientFactory(new InMemoryDiscoveryService(), setter -> { });
    RuntimeScopesExtractor.ConnectionReader undiscoverableReader =
        new ConnectionPropertiesFetcher(emptyDiscoveryFactory).forNamespace("ns1");
    try {
      undiscoverableReader.read("any-conn");
      Assert.fail("Expected IOException when studio service is not discovered");
    } catch (IOException expected) {
      // Expected
    }
  }

  @Test
  public void testSharedLaunchDeadlineAndExtractorFallback() throws Exception {
    AtomicLong clockNanos = new AtomicLong(1_000_000L);
    RemoteClientFactory factory =
        Mockito.spy(new RemoteClientFactory(discoveryService, setter -> { }));
    ConnectionPropertiesFetcher fetcher = new ConnectionPropertiesFetcher(
        factory, TimeUnit.SECONDS.toMillis(15), clockNanos::get);

    RuntimeScopesExtractor.ConnectionReader reader = fetcher.forNamespace("ns1");
    // First read after 10s of 15s budget succeeds with 5s connect/read timeout
    clockNanos.addAndGet(TimeUnit.SECONDS.toNanos(10));
    Optional<Map<String, String>> first = reader.read("conn1");
    Assert.assertTrue(first.isPresent());
    Assert.assertEquals(1, stubHandler.requestCount.get());
    ArgumentCaptor<HttpRequestConfig> configCaptor =
        ArgumentCaptor.forClass(HttpRequestConfig.class);
    Mockito.verify(factory).createRemoteClient(
        Mockito.eq(DISCOVERY_NAME), configCaptor.capture(), Mockito.anyString());
    int expectedTimeoutMs = (int) TimeUnit.SECONDS.toMillis(5);
    Assert.assertEquals(expectedTimeoutMs, configCaptor.getValue().getConnectTimeout());
    Assert.assertEquals(expectedTimeoutMs, configCaptor.getValue().getReadTimeout());

    // Advance past the 15s deadline -> second read fails immediately without hitting HTTP server
    clockNanos.addAndGet(TimeUnit.SECONDS.toNanos(6));
    try {
      reader.read("conn2");
      Assert.fail("Expected IOException once launch deadline elapsed");
    } catch (IOException expected) {
      // Expected
    }
    Assert.assertEquals(1, stubHandler.requestCount.get());

    // Integrated with RuntimeScopesExtractor: deadline exceeded widens scopes and never throws
    ArtifactId sysArtifact =
        new ArtifactId("cdap-data-pipeline", new ArtifactVersion("6.12.0"), ArtifactScope.SYSTEM);
    Plugin plugin = new Plugin(
        Collections.emptyList(), sysArtifact,
        PluginClass.builder().setType("batchsource").setName("Src")
            .setDescription("desc").setClassName("io.cdap.Src").build(),
        PluginProperties.builder().add("c", "${conn(conn1)} ${conn(conn2)}").build());
    ApplicationSpecification appSpec = new DefaultApplicationSpecification(
        "pipe1", "1.0", null, "desc", null, sysArtifact,
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(),
        Collections.singletonMap("src", plugin));
    ProgramId wfId = new NamespaceId("ns1").app("pipe1").workflow("wf");

    // Pre-expired deadline for this launch
    ConnectionPropertiesFetcher expiredFetcher =
        new ConnectionPropertiesFetcher(factory, 0L, clockNanos::get);
    RuntimeScopes widened = RuntimeScopesExtractor.extract(
        appSpec, wfId, Collections.emptyMap(), TriggerMappedArguments.Result.empty(),
        expiredFetcher.forNamespace("ns1"));
    Assert.assertTrue(widened.getConnections().contains(RuntimeScopes.WILDCARD));
    Assert.assertEquals(Collections.singleton(RuntimeScopes.OAUTH_WILDCARD), widened.getOauth());
    Assert.assertTrue(widened.hasExternalDynamicSecret());
  }

  /**
   * Stub HTTP handler simulating pipeline studio connection endpoints.
   */
  @Path("/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/{context}/connections")
  public static final class StudioStubHandler extends AbstractHttpHandler {

    private final List<String> recordedUris = new CopyOnWriteArrayList<>();
    private final AtomicInteger requestCount = new AtomicInteger();

    @GET
    @Path("/{connection}")
    public void getConnection(HttpRequest request, HttpResponder responder,
        @PathParam("context") String context,
        @PathParam("connection") String connection) {
      requestCount.incrementAndGet();
      recordedUris.add(request.uri());
      if ("not-found".equals(connection)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "not found");
        return;
      }
      if ("bad-request".equals(connection)) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "invalid connection name");
        return;
      }
      if ("unauthorized".equals(connection)) {
        responder.sendString(HttpResponseStatus.UNAUTHORIZED, "{\"message\":\"unauthorized\"}");
        return;
      }
      if ("server-error".equals(connection)) {
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "boom");
        return;
      }
      if ("unavailable".equals(connection)) {
        responder.sendString(HttpResponseStatus.SERVICE_UNAVAILABLE, "unavailable");
        return;
      }
      if ("forbidden".equals(connection)) {
        responder.sendString(HttpResponseStatus.FORBIDDEN, "forbidden");
        return;
      }
      if ("bad-json".equals(connection)) {
        responder.sendString(HttpResponseStatus.OK, "{not-valid-json");
        return;
      }
      if ("empty-body".equals(connection)) {
        responder.sendString(HttpResponseStatus.OK, "");
        return;
      }
      if ("no-plugin".equals(connection)) {
        responder.sendString(HttpResponseStatus.OK, "{\"name\":\"no-plugin\"}");
        return;
      }
      if ("no-props".equals(connection)) {
        responder.sendString(HttpResponseStatus.OK, "{\"name\":\"no-props\",\"plugin\":{}}");
        return;
      }
      responder.sendString(HttpResponseStatus.OK,
          "{\"name\":\"c\",\"plugin\":{\"properties\":{\"secret\":\"${secure(s1)}\"}}}");
    }
  }
}
