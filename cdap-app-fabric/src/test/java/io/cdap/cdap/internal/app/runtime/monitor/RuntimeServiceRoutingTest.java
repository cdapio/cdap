/*
 * Copyright © 2020-2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.app.guice.RuntimeServerModule;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ServiceException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.encryption.guice.UserCredentialAeadEncryptionModule;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.NoOpAuditLogModule;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.common.internal.remote.NoOpRemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.service.Services;
import io.cdap.cdap.common.utils.TimeProvider;
import io.cdap.cdap.data2.dataset2.lib.external.ExternalDataset;
import io.cdap.cdap.gateway.handlers.PingHandler;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.DatasetInstanceConfiguration;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.InternalAccessEnforcer;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyConsumer;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.Logger;

/**
 * Unit test for the {@link RuntimeServiceRoutingHandler}.
 */
public class RuntimeServiceRoutingTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final String MOCK_SERVICE = "mock";
  private static final Gson GSON = new Gson();

  private Injector injector;
  private MessagingService messagingService;
  private RuntimeServer runtimeServer;
  private NettyHttpService mockService;
  private Cancellable mockServiceCancellable;
  private MockRemoteAuthenticatorProvider mockRemoteAuthenticatorProvider;
  private AtomicReference<RuntimeRequestValidator> customValidatorRef;

  @Before
  public void beforeTest() throws Exception {
    customValidatorRef = new AtomicReference<>();
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    mockRemoteAuthenticatorProvider = new MockRemoteAuthenticatorProvider();

    injector = Guice.createInjector(
      new ConfigModule(cConf),
      new PrivateModule() {
        @Override
        protected void configure() {
          bind(RemoteAuthenticator.class).toProvider(mockRemoteAuthenticatorProvider);
          expose(RemoteAuthenticator.class);
        }
      },
      new LocalLocationModule(),
      new InMemoryDiscoveryModule(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new AuthorizationEnforcementModule().getNoOpModules(),
      new AuthenticationContextModules().getNoOpModule(),
      new RuntimeServerModule() {
        @Override
        protected void bindRequestValidator() {
          bind(RuntimeRequestValidator.class).toInstance((programRunId, request) -> {
            RuntimeRequestValidator custom = customValidatorRef.get();
            if (custom != null) {
              return custom.validate(programRunId, request);
            }
            String authHeader = request.headers().get(HttpHeaderNames.AUTHORIZATION);
            String expected = "Bearer " + Base64.getEncoder().encodeToString(
              Hashing.md5().hashUnencodedChars(programRunId.toString()).asBytes());
            if (!expected.equals(authHeader)) {
              throw new UnauthenticatedException("Program run " + programRunId + " is not authorized");
            }
            return ValidatedRuntimeRequest.legacy(new ProgramRunInfo(ProgramRunStatus.COMPLETED));
          });
        }

        @Override
        protected void bindLogProcessor() {
          bind(RemoteExecutionLogProcessor.class).toInstance(payloads -> {
          });
        }
      },
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
        }
      },
      new NoOpAuditLogModule(),
      new UserCredentialAeadEncryptionModule()
    );

    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      Services.startAndWait((Service) messagingService);
    }
    messagingService.createTopic(new DefaultTopicMetadata(NamespaceId.SYSTEM.topic("topic")));

    runtimeServer = injector.getInstance(RuntimeServer.class);
    Services.startAndWait(runtimeServer);

    mockService = NettyHttpService.builder(MOCK_SERVICE)
      .setHost(InetAddress.getLocalHost().getCanonicalHostName())
      .setHttpHandlers(new PingHandler(), new MockServiceHandler())
      .build();

    mockService.start();
    mockServiceCancellable = injector.getInstance(DiscoveryService.class)
      .register(URIScheme.createDiscoverable(MOCK_SERVICE, mockService));
  }

  @After
  public void afterTest() throws Exception {
    mockServiceCancellable.cancel();
    mockService.stop();
    Services.stopAndWait(runtimeServer);
    if (messagingService instanceof Service) {
      Services.stopAndWait((Service) messagingService);
    }
  }

  @Test
  public void testGetAndDelete() throws IOException, UnauthorizedException {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app", "1.0").workflow("workflow").run(RunIds.generate());
    mockRemoteAuthenticatorProvider.setAuthenticator(new MockRemoteAuthenticator(programRunId));

    RemoteClient remoteClient = injector.getInstance(RemoteClientFactory.class).createRemoteClient(
      Constants.Service.RUNTIME,
      DefaultHttpRequestConfig.DEFAULT,
      Constants.Gateway.INTERNAL_API_VERSION_3 + "/runtime/namespaces");

    for (HttpMethod method : EnumSet.of(HttpMethod.GET, HttpMethod.DELETE)) {
      for (int status : Arrays.asList(200, 400, 404, 501)) {
        io.cdap.common.http.HttpRequest request =
          remoteClient.requestBuilder(method,
                                      String.format("%s/apps/%s/versions/%s/%s/%s/runs/%s/services/%s/mock/%s/%d",
                                                    programRunId.getNamespace(), programRunId.getApplication(),
                                                    programRunId.getVersion(), programRunId.getType().getCategoryName(),
                                                    programRunId.getProgram(), programRunId.getRun(),
                                                    MOCK_SERVICE, method.name().toLowerCase(), status))
            .build();

        HttpResponse response = remoteClient.execute(request);
        Assert.assertEquals(status, response.getResponseCode());
      }
    }
  }

  @Test
  public void testPutAndPost() throws IOException, UnauthorizedException {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app", "1.0").workflow("workflow").run(RunIds.generate());
    mockRemoteAuthenticatorProvider.setAuthenticator(new MockRemoteAuthenticator(programRunId));

    RemoteClient remoteClient = injector.getInstance(RemoteClientFactory.class).createRemoteClient(
      Constants.Service.RUNTIME,
      DefaultHttpRequestConfig.DEFAULT,
      Constants.Gateway.INTERNAL_API_VERSION_3 + "/runtime/namespaces");
    String largeContent = Strings.repeat("Testing", 32768);

    for (String content : Arrays.asList("", "Small content", largeContent)) {
      for (HttpMethod method : EnumSet.of(HttpMethod.PUT, HttpMethod.POST)) {
        for (int status : Arrays.asList(200, 400, 404, 501)) {
          io.cdap.common.http.HttpRequest request =
            remoteClient.requestBuilder(method,
                                        String.format("%s/apps/%s/versions/%s/%s/%s/runs/%s/services/%s/mock/%s/%d",
                                                      programRunId.getNamespace(), programRunId.getApplication(),
                                                      programRunId.getVersion(),
                                                      programRunId.getType().getCategoryName(),
                                                      programRunId.getProgram(), programRunId.getRun(),
                                                      MOCK_SERVICE, method.name().toLowerCase(), status))
              .withBody(content)
              .build();

          HttpResponse response = remoteClient.execute(request);
          Assert.assertEquals(status, response.getResponseCode());
          Assert.assertEquals(content, response.getResponseBodyAsString(StandardCharsets.UTF_8));
        }
      }
    }
  }

  @Test
  public void testUnauthorized() throws IOException, UnauthorizedException {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app", "1.0").workflow("workflow").run(RunIds.generate());
    RemoteClient remoteClient = injector.getInstance(RemoteClientFactory.class).createRemoteClient(
      Constants.Service.RUNTIME,
      DefaultHttpRequestConfig.DEFAULT,
      Constants.Gateway.INTERNAL_API_VERSION_3 + "/runtime/namespaces");
    io.cdap.common.http.HttpRequest request =
      remoteClient.requestBuilder(HttpMethod.GET,
                                  String.format("%s/apps/%s/versions/%s/%s/%s/runs/%s/services/%s/mock/%s/%d",
                                                programRunId.getNamespace(), programRunId.getApplication(),
                                                programRunId.getVersion(),
                                                programRunId.getType().getCategoryName(),
                                                programRunId.getProgram(), programRunId.getRun(),
                                                MOCK_SERVICE, "get", 200))
        .build();

    HttpResponse response = remoteClient.execute(request);
    Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, response.getResponseCode());
    Assert.assertTrue(
        response.getHeaders().get(Constants.Security.Headers.RUNTIME_TOKEN_FAILURE).isEmpty());
  }

  @Test
  public void testRuntimeTokenFailureHeaderOnRouting() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app", "1.0").workflow("workflow")
        .run(RunIds.generate());
    RemoteClient remoteClient = injector.getInstance(RemoteClientFactory.class).createRemoteClient(
        Constants.Service.RUNTIME,
        DefaultHttpRequestConfig.DEFAULT,
        Constants.Gateway.INTERNAL_API_VERSION_3 + "/runtime/namespaces");
    String path = String.format("%s/apps/%s/versions/%s/%s/%s/runs/%s/services/%s/mock/get/200",
        programRunId.getNamespace(), programRunId.getApplication(), programRunId.getVersion(),
        programRunId.getType().getCategoryName(), programRunId.getProgram(), programRunId.getRun(),
        MOCK_SERVICE);

    // Report-only / allowed 200 omits X-CDAP-Runtime-Token-Failure.
    customValidatorRef.set((runId, req) ->
        ValidatedRuntimeRequest.legacy(new ProgramRunInfo(ProgramRunStatus.RUNNING)));
    assertRoutingResponse(remoteClient, path, HttpURLConnection.HTTP_OK, null);

    // Enforcing LEGACY_TOKEN, URL_MISMATCH, and MISSING_SCOPES emit HTTP 403 + failure header.
    for (RuntimeValidationFailure failure : RuntimeValidationFailure.values()) {
      customValidatorRef.set((runId, req) -> {
        throw new RuntimeTokenRefusedException(failure, "refused: " + failure.getReason());
      });
      assertRoutingResponse(
          remoteClient, path, HttpURLConnection.HTTP_FORBIDDEN, failure.getReason());
    }

    // Generic UnauthorizedException (403) omits X-CDAP-Runtime-Token-Failure.
    customValidatorRef.set((runId, req) -> {
      throw new UnauthorizedException("generic forbidden");
    });
    assertRoutingResponse(remoteClient, path, HttpURLConnection.HTTP_FORBIDDEN, null);

    // UnauthenticatedException (401) omits X-CDAP-Runtime-Token-Failure.
    customValidatorRef.set((runId, req) -> {
      throw new UnauthenticatedException("unauthenticated");
    });
    assertRoutingResponse(remoteClient, path, HttpURLConnection.HTTP_UNAUTHORIZED, null);
  }

  private static void assertRoutingResponse(RemoteClient remoteClient, String path,
      int expectedStatus, @Nullable String expectedFailureReason) throws IOException {
    HttpURLConnection conn = remoteClient.openConnection(HttpMethod.GET, path);
    try {
      Assert.assertEquals(expectedStatus, conn.getResponseCode());
      Assert.assertEquals(expectedFailureReason,
          conn.getHeaderField(Constants.Security.Headers.RUNTIME_TOKEN_FAILURE));
    } finally {
      conn.disconnect();
    }
  }

  @Test
  public void testRuntimeTokenExchangeHeadersAndContextRestoration() throws Exception {
    CConfiguration secConf = CConfiguration.create();
    secConf.setBoolean(Constants.Security.ENABLED, true);
    secConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);

    Injector secInjector = Guice.createInjector(
        new ConfigModule(secConf),
        new IOModule(),
        new CoreSecurityRuntimeModule().getInMemoryModules(),
        new AuthenticationContextModules().getMasterModule());

    TokenManager tokenManager = secInjector.getInstance(TokenManager.class);
    Services.startAndWait(tokenManager);
    try {
      Codec<AccessToken> tokenCodec = secInjector.getInstance(
          Key.get(new TypeLiteral<Codec<AccessToken>>() { }));
      InternalAuthenticator internalAuthenticator =
          secInjector.getInstance(DefaultInternalAuthenticator.class);
      InternalAccessEnforcer internalAccessEnforcer =
          secInjector.getInstance(InternalAccessEnforcer.class);

      ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf")
          .run(RunIds.generate());
      long now = System.currentTimeMillis();
      AccessToken runtimeAccessToken = tokenManager.signIdentifier(new UserIdentity(
          GSON.toJson(runId), UserIdentity.IdentifierType.RUNTIME,
          Collections.emptyList(), now, now + TimeUnit.MINUTES.toMillis(5)));
      String callerTokenValue = Base64.getEncoder().encodeToString(
          tokenCodec.encode(runtimeAccessToken));
      String callerTokenHeader = Credential.CREDENTIAL_TYPE_INTERNAL + " " + callerTokenValue;

      AtomicReference<Map<String, String>> capturedHeaders = new AtomicReference<>();
      AtomicReference<String> capturedBody = new AtomicReference<>();
      NettyHttpService captureService = NettyHttpService.builder("capture")
          .setHost(InetAddress.getLocalHost().getCanonicalHostName())
          .setHttpHandlers(new HeaderCaptureHandler(capturedHeaders, capturedBody))
          .build();
      captureService.start();
      Cancellable captureReg = injector.getInstance(DiscoveryService.class)
          .register(URIScheme.createDiscoverable("capture", captureService));

      try {
        RuntimeRequestValidator runtimeValidator = (id, req) ->
            ValidatedRuntimeRequest.runtimeToken(
                new ProgramRunInfo(ProgramRunStatus.RUNNING), runId, null);

        CConfiguration allowlistConf = CConfiguration.create();
        allowlistConf.setBoolean(Constants.RuntimeMonitor.RUNTIME_TOKEN_VALIDATION_ENABLED, true);
        allowlistConf.set(Constants.RuntimeMonitor.RUNTIME_TOKEN_ALLOWLIST_EXTRA_RULES,
            "[{\"service\":\"capture\",\"methods\":[\"GET\",\"POST\"],\"pathTemplate\":\"/echo\"},"
                + "{\"service\":\"nonexistent-service\",\"methods\":[\"GET\"],"
                + "\"pathTemplate\":\"/echo\"}]");
        RuntimeCallbackAllowlist allowlist = new RuntimeCallbackAllowlist(allowlistConf);
        RuntimeValidationReporter validationReporter = new RuntimeValidationReporter(
            allowlistConf, new NoOpMetricsCollectionService());

        // Capture exact headers constructed before HttpURLConnection adds transport headers.
        AtomicReference<HttpHeaders> forwardedRequestHeaders = new AtomicReference<>();
        RuntimeServiceRoutingHandler handler = new RuntimeServiceRoutingHandler(
            injector.getInstance(DiscoveryServiceClient.class),
            runtimeValidator,
            internalAuthenticator,
            allowlist,
            validationReporter) {
          @Override
          public void routeService(HttpRequest request, HttpResponder responder,
              String service, String path) throws Exception {
            forwardedRequestHeaders.set(request.headers());
            super.routeService(request, responder, service, path);
          }

          @Override
          public BodyConsumer routeServiceWithBody(HttpRequest request, HttpResponder responder,
              String service, String path) throws Exception {
            forwardedRequestHeaders.set(request.headers());
            return super.routeServiceWithBody(request, responder, service, path);
          }
        };

        String uri = String.format(
            "%s/runtime/namespaces/%s/apps/%s/versions/%s/%s/%s/runs/%s/services/capture/echo",
            Constants.Gateway.INTERNAL_API_VERSION_3,
            runId.getNamespace(), runId.getApplication(), runId.getVersion(),
            runId.getType().getCategoryName(), runId.getProgram(), runId.getRun());

        Credential callerCred = new Credential(callerTokenValue, Credential.CredentialType.INTERNAL);
        SecurityRequestContext.reset();
        SecurityRequestContext.setUserId("caller-user");
        SecurityRequestContext.setUserIp("10.1.2.3");
        SecurityRequestContext.setUserCredential(callerCred);
        try {
          // 1. GET request with allowed + disallowed headers
          DefaultHttpRequest getReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
              io.netty.handler.codec.http.HttpMethod.GET, uri);
          getReq.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
          getReq.headers().set(HttpHeaderNames.IF_MODIFIED_SINCE, "Wed, 21 Oct 2015 07:28:00 GMT");
          getReq.headers().set(Constants.Security.Headers.RUNTIME_TOKEN, callerTokenHeader);
          getReq.headers().set(Constants.Security.Headers.USER_ID, "caller-user");
          getReq.headers().set(Constants.Security.Headers.USER_IP, "10.1.2.3");
          getReq.headers().set("X-Custom-Header", "should-not-forward");
          getReq.headers().set(HttpHeaderNames.COOKIE, "secret=1");

          HttpResponder responder = mockResponder();
          handler.routeService(getReq, responder,
              runId.getNamespace(), runId.getApplication(), runId.getVersion(),
              runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), "capture");

          // Verify exact header set on the forwarded request object
          Set<String> expectedNames = new HashSet<>(Arrays.asList(
              HttpHeaderNames.CONTENT_TYPE.toString().toLowerCase(),
              HttpHeaderNames.IF_MODIFIED_SINCE.toString().toLowerCase(),
              Constants.Security.Headers.RUNTIME_TOKEN.toLowerCase(),
              Constants.Security.Headers.USER_ID.toLowerCase()));
          Set<String> actualNames = new HashSet<>();
          for (String name : forwardedRequestHeaders.get().names()) {
            actualNames.add(name.toLowerCase());
          }
          Assert.assertEquals(expectedNames, actualNames);

          // Verify downstream received the exact exchanged headers and no leaked headers
          Map<String, String> received = capturedHeaders.get();
          Assert.assertNotNull(received);
          Assert.assertEquals("application/json", received.get("content-type"));
          Assert.assertEquals("Wed, 21 Oct 2015 07:28:00 GMT", received.get("if-modified-since"));
          Assert.assertEquals(Constants.Security.Authentication.RUNTIME_IDENTITY,
              received.get(Constants.Security.Headers.USER_ID.toLowerCase()));
          Assert.assertNull(received.get("x-custom-header"));
          Assert.assertNull(received.get("cookie"));
          Assert.assertNull(received.get(Constants.Security.Headers.USER_IP.toLowerCase()));

          String forwardedTokenHeader = received.get(
              Constants.Security.Headers.RUNTIME_TOKEN.toLowerCase());
          Assert.assertNotNull(forwardedTokenHeader);
          Assert.assertNotEquals(callerTokenHeader, forwardedTokenHeader);
          Assert.assertTrue(forwardedTokenHeader.startsWith(Credential.CREDENTIAL_TYPE_INTERNAL + " "));
          String forwardedTokenValue = forwardedTokenHeader.substring(
              (Credential.CREDENTIAL_TYPE_INTERNAL + " ").length());
          // Must be accepted by InternalAccessEnforcer
          internalAccessEnforcer.enforce(InstanceId.SELF,
              new Principal(Constants.Security.Authentication.RUNTIME_IDENTITY,
                  Principal.PrincipalType.USER,
                  new Credential(forwardedTokenValue, Credential.CredentialType.INTERNAL)),
              StandardPermission.GET);

          // Verify SecurityRequestContext was restored
          Assert.assertEquals("caller-user", SecurityRequestContext.getUserId());
          Assert.assertEquals("10.1.2.3", SecurityRequestContext.getUserIp());
          Assert.assertEquals(callerCred, SecurityRequestContext.getUserCredential());

          // 1b. GET request without optional headers forwards strictly the 2 internal auth headers
          DefaultHttpRequest minimalGetReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
              io.netty.handler.codec.http.HttpMethod.GET, uri);
          minimalGetReq.headers().set(Constants.Security.Headers.RUNTIME_TOKEN, callerTokenHeader);
          minimalGetReq.headers().set("X-Only-Caller", "drop");
          handler.routeService(minimalGetReq, responder,
              runId.getNamespace(), runId.getApplication(), runId.getVersion(),
              runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), "capture");
          Set<String> minimalActualNames = new HashSet<>();
          for (String name : forwardedRequestHeaders.get().names()) {
            minimalActualNames.add(name.toLowerCase());
          }
          Assert.assertEquals(new HashSet<>(Arrays.asList(
              Constants.Security.Headers.RUNTIME_TOKEN.toLowerCase(),
              Constants.Security.Headers.USER_ID.toLowerCase())), minimalActualNames);

          // 2. POST request with body: exchange happens before body is read, body relayed cleanly
          DefaultHttpRequest postReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
              io.netty.handler.codec.http.HttpMethod.POST, uri);
          postReq.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
          postReq.headers().set(Constants.Security.Headers.RUNTIME_TOKEN, callerTokenHeader);
          postReq.headers().set("X-Extra", "drop");
          BodyConsumer consumer = handler.routeServiceWithBody(postReq, responder,
              runId.getNamespace(), runId.getApplication(), runId.getVersion(),
              runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), "capture");
          consumer.chunk(Unpooled.copiedBuffer("hello-exchange", StandardCharsets.UTF_8), responder);
          consumer.finished(responder);
          Assert.assertEquals("hello-exchange", capturedBody.get());
          Assert.assertNull(capturedHeaders.get().get("x-extra"));
          Assert.assertEquals("caller-user", SecurityRequestContext.getUserId());
          Assert.assertEquals("10.1.2.3", SecurityRequestContext.getUserIp());
          Assert.assertEquals(callerCred, SecurityRequestContext.getUserCredential());

          // 3. Exception during forwarding still leaves SecurityRequestContext restored
          String missingUri = uri.replace("/services/capture/", "/services/nonexistent-service/");
          DefaultHttpRequest missingReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
              io.netty.handler.codec.http.HttpMethod.GET, missingUri);
          missingReq.headers().set(Constants.Security.Headers.RUNTIME_TOKEN, callerTokenHeader);
          try {
            handler.routeService(missingReq, responder,
                runId.getNamespace(), runId.getApplication(), runId.getVersion(),
                runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
                "nonexistent-service");
            Assert.fail("Expected ServiceUnavailableException");
          } catch (ServiceUnavailableException expected) {
            // expected
          }
          Assert.assertEquals("caller-user", SecurityRequestContext.getUserId());
          Assert.assertEquals("10.1.2.3", SecurityRequestContext.getUserIp());
          Assert.assertEquals(callerCred, SecurityRequestContext.getUserCredential());

          // 4. Exception thrown inside InternalAuthenticator also restores SecurityRequestContext
          RuntimeServiceRoutingHandler failingAuthHandler = new RuntimeServiceRoutingHandler(
              injector.getInstance(DiscoveryServiceClient.class),
              runtimeValidator,
              headerSetter -> {
                Assert.assertNull(SecurityRequestContext.getUserId());
                Assert.assertNull(SecurityRequestContext.getUserIp());
                Assert.assertNull(SecurityRequestContext.getUserCredential());
                throw new RuntimeException("mint-failed");
              },
              allowlist,
              validationReporter);
          try {
            failingAuthHandler.routeService(getReq, responder,
                runId.getNamespace(), runId.getApplication(), runId.getVersion(),
                runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), "capture");
            Assert.fail("Expected RuntimeException");
          } catch (RuntimeException expected) {
            Assert.assertEquals("mint-failed", expected.getMessage());
          }
          Assert.assertEquals("caller-user", SecurityRequestContext.getUserId());
          Assert.assertEquals("10.1.2.3", SecurityRequestContext.getUserIp());
          Assert.assertEquals(callerCred, SecurityRequestContext.getUserCredential());

          // 5. Legacy-validated request forwards the original request headers unchanged
          RuntimeServiceRoutingHandler legacyHandler = new RuntimeServiceRoutingHandler(
              injector.getInstance(DiscoveryServiceClient.class),
              (id, req) -> ValidatedRuntimeRequest.legacy(
                  new ProgramRunInfo(ProgramRunStatus.RUNNING)),
              internalAuthenticator,
              allowlist,
              validationReporter);
          capturedHeaders.set(null);
          legacyHandler.routeService(getReq, responder,
              runId.getNamespace(), runId.getApplication(), runId.getVersion(),
              runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), "capture");
          Map<String, String> legacyReceived = capturedHeaders.get();
          Assert.assertNotNull(legacyReceived);
          Assert.assertEquals("should-not-forward", legacyReceived.get("x-custom-header"));
          Assert.assertEquals(callerTokenHeader,
              legacyReceived.get(Constants.Security.Headers.RUNTIME_TOKEN.toLowerCase()));
        } finally {
          SecurityRequestContext.reset();
        }
      } finally {
        captureReg.cancel();
        captureService.stop();
      }
    } finally {
      Services.stopAndWait(tokenManager);
    }
  }

  @Test
  public void testAllowlistEnforcingAndReportOnly() throws Exception {
    ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf")
        .run(RunIds.generate());
    RuntimeRequestValidator runtimeValidator = (id, req) ->
        ValidatedRuntimeRequest.runtimeToken(
            new ProgramRunInfo(ProgramRunStatus.RUNNING), runId, null);

    AtomicReference<Map<String, String>> capturedHeaders = new AtomicReference<>();
    AtomicReference<String> capturedBody = new AtomicReference<>();
    NettyHttpService captureService = NettyHttpService.builder("unlisted-svc")
        .setHost(InetAddress.getLocalHost().getCanonicalHostName())
        .setHttpHandlers(new HeaderCaptureHandler(capturedHeaders, capturedBody))
        .build();
    captureService.start();
    Cancellable captureReg = injector.getInstance(DiscoveryService.class)
        .register(URIScheme.createDiscoverable("unlisted-svc", captureService));

    AtomicInteger datasetCalls = new AtomicInteger();
    AtomicReference<String> capturedDatasetBody = new AtomicReference<>();
    AtomicReference<Map<String, String>> capturedDatasetHeaders = new AtomicReference<>();
    NettyHttpService datasetService = NettyHttpService.builder(Constants.Service.DATASET_MANAGER)
        .setHost(InetAddress.getLocalHost().getCanonicalHostName())
        .setHttpHandlers(new DatasetCaptureHandler(
            datasetCalls, capturedDatasetBody, capturedDatasetHeaders))
        .build();
    datasetService.start();
    Cancellable datasetReg = injector.getInstance(DiscoveryService.class)
        .register(URIScheme.createDiscoverable(Constants.Service.DATASET_MANAGER, datasetService));

    try {
      String encodedPathUri = String.format(
          "%s/runtime/namespaces/%s/apps/%s/versions/%s/%s/%s/runs/%s/services/unlisted-svc"
              + "/echo%%20path?secret=hidden",
          Constants.Gateway.INTERNAL_API_VERSION_3,
          runId.getNamespace(), runId.getApplication(), runId.getVersion(),
          runId.getType().getCategoryName(), runId.getProgram(), runId.getRun());

      // Enforcing mode: disallowed GET throws UnauthorizedException, counts, logs decoded path
      Map<String, Long> enforcingCounters = new HashMap<>();
      Logger enforcingLogger = Mockito.mock(Logger.class);
      RuntimeServiceRoutingHandler enforcingHandler = new RuntimeServiceRoutingHandler(
          injector.getInstance(DiscoveryServiceClient.class),
          runtimeValidator,
          headerSetter -> headerSetter.accept(Constants.Security.Headers.USER_ID, "system"),
          new RuntimeCallbackAllowlist(CConfiguration.create()),
          createReporter(true, enforcingCounters, enforcingLogger));

      DefaultHttpRequest disallowedGet = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
          io.netty.handler.codec.http.HttpMethod.GET, encodedPathUri);
      HttpResponder responder = mockResponder();
      try {
        enforcingHandler.routeService(disallowedGet, responder,
            runId.getNamespace(), runId.getApplication(), runId.getVersion(),
            runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), "unlisted-svc");
        Assert.fail("Expected UnauthorizedException for disallowed GET");
      } catch (UnauthorizedException expected) {
        // expected
      }
      Assert.assertNull(capturedHeaders.get());
      Assert.assertEquals(Long.valueOf(1L),
          enforcingCounters.get(RuntimeValidationFailure.API_NOT_ALLOWED.getMetricName()));
      Mockito.verify(enforcingLogger).debug(Mockito.anyString(),
          Mockito.eq("api_not_allowed"), Mockito.eq(runId), Mockito.eq("unlisted-svc"),
          Mockito.eq("GET"), Mockito.eq("/echo path"), Mockito.eq(true));

      // Enforcing mode: disallowed POST via routeServiceWithBody throws UnauthorizedException
      DefaultHttpRequest disallowedPost = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
          io.netty.handler.codec.http.HttpMethod.POST, encodedPathUri);
      try {
        enforcingHandler.routeServiceWithBody(disallowedPost, responder,
            runId.getNamespace(), runId.getApplication(), runId.getVersion(),
            runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), "unlisted-svc");
        Assert.fail("Expected UnauthorizedException for disallowed POST");
      } catch (UnauthorizedException expected) {
        // expected
      }
      Assert.assertNull(capturedBody.get());
      Assert.assertEquals(Long.valueOf(2L),
          enforcingCounters.get(RuntimeValidationFailure.API_NOT_ALLOWED.getMetricName()));
      Mockito.verify(enforcingLogger).debug(Mockito.anyString(),
          Mockito.eq("api_not_allowed"), Mockito.eq(runId), Mockito.eq("unlisted-svc"),
          Mockito.eq("POST"), Mockito.eq("/echo path"), Mockito.eq(true));

      // Enforcing mode: wrong-namespace dataset PUT via routeServiceWithBody throws 403 and logs
      // matched template
      String dsBasePrefix = String.format(
          "%s/runtime/namespaces/%s/apps/%s/versions/%s/%s/%s/runs/%s/services/%s",
          Constants.Gateway.INTERNAL_API_VERSION_3,
          runId.getNamespace(), runId.getApplication(), runId.getVersion(),
          runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
          Constants.Service.DATASET_MANAGER);
      DefaultHttpRequest wrongNsPut = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
          io.netty.handler.codec.http.HttpMethod.PUT,
          dsBasePrefix + "/v3/namespaces/otherns/data/datasets/secret-ds");
      try {
        enforcingHandler.routeServiceWithBody(wrongNsPut, responder,
            runId.getNamespace(), runId.getApplication(), runId.getVersion(),
            runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
            Constants.Service.DATASET_MANAGER);
        Assert.fail("Expected UnauthorizedException for wrong-namespace dataset PUT");
      } catch (UnauthorizedException expected) {
        // expected
      }
      Assert.assertEquals(0, datasetCalls.get());
      Assert.assertEquals(Long.valueOf(3L),
          enforcingCounters.get(RuntimeValidationFailure.API_NOT_ALLOWED.getMetricName()));
      Mockito.verify(enforcingLogger).debug(Mockito.anyString(),
          Mockito.eq("api_not_allowed"), Mockito.eq(runId),
          Mockito.eq(Constants.Service.DATASET_MANAGER), Mockito.eq("PUT"),
          Mockito.eq("/v3/namespaces/{namespace}/data/datasets/{name}"), Mockito.eq(true));

      // Enforcing mode: workflow-local dataset DELETE allowed; foreign dataset DELETE denied
      DefaultHttpRequest allowedDelete = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
          io.netty.handler.codec.http.HttpMethod.DELETE,
          dsBasePrefix + "/v3/namespaces/default/data/datasets/conn-1." + runId.getRun());
      enforcingHandler.routeService(allowedDelete, responder,
          runId.getNamespace(), runId.getApplication(), runId.getVersion(),
          runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
          Constants.Service.DATASET_MANAGER);
      Assert.assertEquals(1, datasetCalls.get());

      DefaultHttpRequest foreignDelete = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
          io.netty.handler.codec.http.HttpMethod.DELETE,
          dsBasePrefix + "/v3/namespaces/default/data/datasets/prod-table");
      try {
        enforcingHandler.routeService(foreignDelete, responder,
            runId.getNamespace(), runId.getApplication(), runId.getVersion(),
            runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
            Constants.Service.DATASET_MANAGER);
        Assert.fail("Expected UnauthorizedException for foreign dataset DELETE");
      } catch (UnauthorizedException expected) {
        // expected
      }
      Assert.assertEquals(1, datasetCalls.get());
      Mockito.verify(enforcingLogger).debug(Mockito.anyString(),
          Mockito.eq("api_not_allowed"), Mockito.eq(runId),
          Mockito.eq(Constants.Service.DATASET_MANAGER), Mockito.eq("DELETE"),
          Mockito.eq("/v3/namespaces/{namespace}/data/datasets/{name}"), Mockito.eq(true));

      // Report-only mode: forwards disallowed GET & POST, counts, and logs decoded path w/o query
      Map<String, Long> reportOnlyCounters = new HashMap<>();
      Logger reportOnlyLogger = Mockito.mock(Logger.class);
      RuntimeServiceRoutingHandler reportOnlyHandler = new RuntimeServiceRoutingHandler(
          injector.getInstance(DiscoveryServiceClient.class),
          runtimeValidator,
          headerSetter -> headerSetter.accept(Constants.Security.Headers.USER_ID, "system"),
          new RuntimeCallbackAllowlist(CConfiguration.create()),
          createReporter(false, reportOnlyCounters, reportOnlyLogger));

      reportOnlyHandler.routeService(disallowedGet, responder,
          runId.getNamespace(), runId.getApplication(), runId.getVersion(),
          runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), "unlisted-svc");
      Assert.assertNotNull(capturedHeaders.get());
      Assert.assertEquals(Constants.Security.Authentication.RUNTIME_IDENTITY,
          capturedHeaders.get().get(Constants.Security.Headers.USER_ID.toLowerCase()));
      Assert.assertEquals(Long.valueOf(1L),
          reportOnlyCounters.get(RuntimeValidationFailure.API_NOT_ALLOWED.getMetricName()));
      Mockito.verify(reportOnlyLogger).debug(Mockito.anyString(),
          Mockito.eq("api_not_allowed"), Mockito.eq(runId), Mockito.eq("unlisted-svc"),
          Mockito.eq("GET"), Mockito.eq("/echo path"), Mockito.eq(false));

      BodyConsumer postConsumer = reportOnlyHandler.routeServiceWithBody(disallowedPost, responder,
          runId.getNamespace(), runId.getApplication(), runId.getVersion(),
          runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), "unlisted-svc");
      postConsumer.chunk(Unpooled.copiedBuffer("report-only-body", StandardCharsets.UTF_8),
          responder);
      postConsumer.finished(responder);
      Assert.assertEquals("report-only-body", capturedBody.get());
      Assert.assertEquals(Long.valueOf(2L),
          reportOnlyCounters.get(RuntimeValidationFailure.API_NOT_ALLOWED.getMetricName()));

      // Report-only mode with matched template (wrong namespace): forwards, counts, logs template
      DefaultHttpRequest reportWrongNsGet = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
          io.netty.handler.codec.http.HttpMethod.GET,
          dsBasePrefix + "/v3/namespaces/otherns/data/datasets/secret-ds");
      reportOnlyHandler.routeService(reportWrongNsGet, responder,
          runId.getNamespace(), runId.getApplication(), runId.getVersion(),
          runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
          Constants.Service.DATASET_MANAGER);
      Assert.assertEquals(2, datasetCalls.get());
      Assert.assertEquals(Long.valueOf(3L),
          reportOnlyCounters.get(RuntimeValidationFailure.API_NOT_ALLOWED.getMetricName()));
      Mockito.verify(reportOnlyLogger).debug(Mockito.anyString(),
          Mockito.eq("api_not_allowed"), Mockito.eq(runId),
          Mockito.eq(Constants.Service.DATASET_MANAGER), Mockito.eq("GET"),
          Mockito.eq("/v3/namespaces/{namespace}/data/datasets/{name}"), Mockito.eq(false));
    } finally {
      datasetReg.cancel();
      datasetService.stop();
      captureReg.cancel();
      captureService.stop();
    }
  }

  @Test
  public void testDatasetPutBufferingBodyConsumer() throws Exception {
    ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf")
        .run(RunIds.generate());
    RuntimeRequestValidator runtimeValidator = (id, req) ->
        ValidatedRuntimeRequest.runtimeToken(
            new ProgramRunInfo(ProgramRunStatus.RUNNING), runId, null);
    InternalAuthenticator stubAuthenticator =
        headerSetter -> headerSetter.accept(Constants.Security.Headers.RUNTIME_TOKEN,
            "CDAP-Internal exchanged");

    AtomicInteger downstreamCalls = new AtomicInteger();
    AtomicReference<String> capturedDatasetBody = new AtomicReference<>();
    AtomicReference<Map<String, String>> capturedDatasetHeaders = new AtomicReference<>();
    NettyHttpService datasetService = NettyHttpService.builder(Constants.Service.DATASET_MANAGER)
        .setHost(InetAddress.getLocalHost().getCanonicalHostName())
        .setHttpHandlers(new DatasetCaptureHandler(
            downstreamCalls, capturedDatasetBody, capturedDatasetHeaders))
        .build();
    datasetService.start();
    Cancellable datasetReg = injector.getInstance(DiscoveryService.class)
        .register(URIScheme.createDiscoverable(Constants.Service.DATASET_MANAGER, datasetService));

    try {
      String basePrefix = String.format(
          "%s/runtime/namespaces/%s/apps/%s/versions/%s/%s/%s/runs/%s/services/%s",
          Constants.Gateway.INTERNAL_API_VERSION_3,
          runId.getNamespace(), runId.getApplication(), runId.getVersion(),
          runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
          Constants.Service.DATASET_MANAGER);

      Map<String, Long> enforcingCounters = new HashMap<>();
      Logger enforcingLogger = Mockito.mock(Logger.class);
      RuntimeCallbackAllowlist allowlist = new RuntimeCallbackAllowlist(CConfiguration.create());
      RuntimeServiceRoutingHandler enforcingHandler = new RuntimeServiceRoutingHandler(
          injector.getInstance(DiscoveryServiceClient.class),
          runtimeValidator,
          stubAuthenticator,
          allowlist,
          createReporter(true, enforcingCounters, enforcingLogger),
          128);

      // 1. Legitimate externalDataset PUT with base.path across multiple chunks is forwarded
      String extUri = basePrefix + "/v3/namespaces/default/data/datasets/ext-ds";
      String extBody = GSON.toJson(new DatasetInstanceConfiguration(
          ExternalDataset.TYPE,
          Collections.singletonMap(FileSetProperties.BASE_PATH, "/data/ext")));
      DefaultHttpRequest extReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
          io.netty.handler.codec.http.HttpMethod.PUT, extUri);
      extReq.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
      extReq.headers().set("X-Caller-Secret", "drop-me");

      HttpResponder okResponder = mockResponder();
      BodyConsumer consumer = enforcingHandler.routeServiceWithBody(extReq, okResponder,
          runId.getNamespace(), runId.getApplication(), runId.getVersion(),
          runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
          Constants.Service.DATASET_MANAGER);

      int split = extBody.length() / 2;
      ByteBuf chunk1 = Unpooled.copiedBuffer(extBody.substring(0, split), StandardCharsets.UTF_8);
      ByteBuf chunk2 = Unpooled.copiedBuffer(extBody.substring(split), StandardCharsets.UTF_8);
      try {
        consumer.chunk(chunk1, okResponder);
        consumer.chunk(chunk2, okResponder);
        Assert.assertEquals(2, chunk1.refCnt());
        Assert.assertEquals(2, chunk2.refCnt());
        consumer.finished(okResponder);
        Assert.assertEquals(1, chunk1.refCnt());
        Assert.assertEquals(1, chunk2.refCnt());
      } finally {
        chunk1.release();
        chunk2.release();
      }
      Assert.assertEquals(1, downstreamCalls.get());
      Assert.assertEquals(extBody, capturedDatasetBody.get());
      Assert.assertNull(capturedDatasetHeaders.get().get("x-caller-secret"));
      Assert.assertEquals("CDAP-Internal exchanged",
          capturedDatasetHeaders.get().get(Constants.Security.Headers.RUNTIME_TOKEN.toLowerCase()));
      Mockito.verify(okResponder).sendContent(Mockito.eq(HttpResponseStatus.OK),
          Mockito.any(BodyProducer.class), Mockito.any(HttpHeaders.class));

      // 1b. Exact maxBufferedBodyBytes (128 bytes) workflow-local PUT passes
      String localUri = basePrefix + "/v3/namespaces/default/data/datasets/conn-1."
          + runId.getRun();
      String baseLocalJson = GSON.toJson(new DatasetInstanceConfiguration(
          "table", Collections.singletonMap("k", "")));
      int padLen = 128 - baseLocalJson.getBytes(StandardCharsets.UTF_8).length;
      String exact128Body = GSON.toJson(new DatasetInstanceConfiguration(
          "table", Collections.singletonMap("k", Strings.repeat("x", padLen))));
      Assert.assertEquals(128, exact128Body.getBytes(StandardCharsets.UTF_8).length);
      DefaultHttpRequest localReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
          io.netty.handler.codec.http.HttpMethod.PUT, localUri);
      HttpResponder localResponder = mockResponder();
      BodyConsumer localConsumer = enforcingHandler.routeServiceWithBody(localReq, localResponder,
          runId.getNamespace(), runId.getApplication(), runId.getVersion(),
          runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
          Constants.Service.DATASET_MANAGER);
      ByteBuf exactBuf = Unpooled.copiedBuffer(exact128Body, StandardCharsets.UTF_8);
      try {
        localConsumer.chunk(exactBuf, localResponder);
        localConsumer.finished(localResponder);
        Assert.assertEquals(1, exactBuf.refCnt());
      } finally {
        exactBuf.release();
      }
      Assert.assertEquals(2, downstreamCalls.get());
      Assert.assertEquals(exact128Body, capturedDatasetBody.get());

      // 2. Forbidden FileSet property on defaultCheckpointDataset -> 403, downstream not called
      String cpUri = basePrefix + "/v3/namespaces/default/data/datasets/"
          + RuntimeCallbackAllowlist.DEFAULT_CHECKPOINT_DATASET;
      String badBody = GSON.toJson(new DatasetInstanceConfiguration(
          FileSet.TYPE, Collections.singletonMap("files.base.path", "/etc")));
      DefaultHttpRequest badReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
          io.netty.handler.codec.http.HttpMethod.PUT, cpUri);
      HttpResponder forbiddenResponder = mockResponder();
      BodyConsumer badConsumer = enforcingHandler.routeServiceWithBody(badReq, forbiddenResponder,
          runId.getNamespace(), runId.getApplication(), runId.getVersion(),
          runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
          Constants.Service.DATASET_MANAGER);
      ByteBuf badBuf = Unpooled.copiedBuffer(badBody, StandardCharsets.UTF_8);
      try {
        badConsumer.chunk(badBuf, forbiddenResponder);
        badConsumer.finished(forbiddenResponder);
        Assert.assertEquals(1, badBuf.refCnt());
      } finally {
        badBuf.release();
      }
      Assert.assertEquals(2, downstreamCalls.get());
      Mockito.verify(forbiddenResponder).sendString(
          Mockito.eq(HttpResponseStatus.FORBIDDEN), Mockito.anyString());
      Assert.assertEquals(Long.valueOf(1L),
          enforcingCounters.get(RuntimeValidationFailure.API_NOT_ALLOWED.getMetricName()));
      Mockito.verify(enforcingLogger).debug(Mockito.anyString(),
          Mockito.eq("api_not_allowed"), Mockito.eq(runId),
          Mockito.eq(Constants.Service.DATASET_MANAGER), Mockito.eq("PUT"),
          Mockito.eq("/v3/namespaces/{namespace}/data/datasets/{name}"), Mockito.eq(true));

      // 3. Report-only mode forwards forbidden body, counts, and logs matched template
      Map<String, Long> reportCounters = new HashMap<>();
      Logger reportLogger = Mockito.mock(Logger.class);
      RuntimeServiceRoutingHandler reportHandler = new RuntimeServiceRoutingHandler(
          injector.getInstance(DiscoveryServiceClient.class),
          runtimeValidator,
          stubAuthenticator,
          allowlist,
          createReporter(false, reportCounters, reportLogger),
          128);
      HttpResponder reportResponder = mockResponder();
      BodyConsumer reportConsumer = reportHandler.routeServiceWithBody(badReq, reportResponder,
          runId.getNamespace(), runId.getApplication(), runId.getVersion(),
          runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
          Constants.Service.DATASET_MANAGER);
      ByteBuf reportBuf = Unpooled.copiedBuffer(badBody, StandardCharsets.UTF_8);
      try {
        reportConsumer.chunk(reportBuf, reportResponder);
        reportConsumer.finished(reportResponder);
        Assert.assertEquals(1, reportBuf.refCnt());
      } finally {
        reportBuf.release();
      }
      Assert.assertEquals(3, downstreamCalls.get());
      Assert.assertEquals(badBody, capturedDatasetBody.get());
      Assert.assertEquals(Long.valueOf(1L),
          reportCounters.get(RuntimeValidationFailure.API_NOT_ALLOWED.getMetricName()));
      Mockito.verify(reportLogger).debug(Mockito.anyString(),
          Mockito.eq("api_not_allowed"), Mockito.eq(runId),
          Mockito.eq(Constants.Service.DATASET_MANAGER), Mockito.eq("PUT"),
          Mockito.eq("/v3/namespaces/{namespace}/data/datasets/{name}"), Mockito.eq(false));

      // 4. Oversized body (> 128 bytes, 3 chunks) in enforcing mode -> 403, buffers released,
      //    subsequent chunks ignored, downstream not called
      HttpResponder oversizedResponder = mockResponder();
      BodyConsumer oversizedConsumer = enforcingHandler.routeServiceWithBody(
          extReq, oversizedResponder,
          runId.getNamespace(), runId.getApplication(), runId.getVersion(),
          runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
          Constants.Service.DATASET_MANAGER);
      ByteBuf firstPart = Unpooled.copiedBuffer(Strings.repeat("a", 80), StandardCharsets.UTF_8);
      ByteBuf overflowPart = Unpooled.copiedBuffer(Strings.repeat("b", 80), StandardCharsets.UTF_8);
      ByteBuf trailingPart = Unpooled.copiedBuffer(Strings.repeat("c", 40), StandardCharsets.UTF_8);
      try {
        oversizedConsumer.chunk(firstPart, oversizedResponder);
        Assert.assertEquals(2, firstPart.refCnt());
        oversizedConsumer.chunk(overflowPart, oversizedResponder);
        Assert.assertEquals(1, firstPart.refCnt());
        Assert.assertEquals(1, overflowPart.refCnt());
        oversizedConsumer.chunk(trailingPart, oversizedResponder);
        Assert.assertEquals(1, trailingPart.refCnt());
        oversizedConsumer.finished(oversizedResponder);
      } finally {
        firstPart.release();
        overflowPart.release();
        trailingPart.release();
      }
      Assert.assertEquals(3, downstreamCalls.get());
      Mockito.verify(oversizedResponder).sendString(
          Mockito.eq(HttpResponseStatus.FORBIDDEN), Mockito.anyString());
      Assert.assertEquals(Long.valueOf(2L),
          enforcingCounters.get(RuntimeValidationFailure.API_NOT_ALLOWED.getMetricName()));

      // 4b. Oversized body (> 128 bytes, 3 chunks) in report-only mode -> counts once, switches to
      //     streaming pass-through, releases buffer, forwards complete body intact
      String largeExtBody = GSON.toJson(new DatasetInstanceConfiguration(
          ExternalDataset.TYPE,
          Collections.singletonMap("big", Strings.repeat("z", 160))));
      Assert.assertTrue(largeExtBody.length() > 128);
      HttpResponder reportOversizedResponder = mockResponder();
      BodyConsumer reportOversizedConsumer = reportHandler.routeServiceWithBody(
          extReq, reportOversizedResponder,
          runId.getNamespace(), runId.getApplication(), runId.getVersion(),
          runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
          Constants.Service.DATASET_MANAGER);
      ByteBuf rPart1 = Unpooled.copiedBuffer(largeExtBody.substring(0, 80), StandardCharsets.UTF_8);
      ByteBuf rPart2 = Unpooled.copiedBuffer(largeExtBody.substring(80, 160),
          StandardCharsets.UTF_8);
      ByteBuf rPart3 = Unpooled.copiedBuffer(largeExtBody.substring(160), StandardCharsets.UTF_8);
      try {
        reportOversizedConsumer.chunk(rPart1, reportOversizedResponder);
        Assert.assertEquals(2, rPart1.refCnt());
        reportOversizedConsumer.chunk(rPart2, reportOversizedResponder);
        // Buffered first part flushed & released when streaming starts; later parts not retained.
        Assert.assertEquals(1, rPart1.refCnt());
        Assert.assertEquals(1, rPart2.refCnt());
        reportOversizedConsumer.chunk(rPart3, reportOversizedResponder);
        Assert.assertEquals(1, rPart3.refCnt());
        reportOversizedConsumer.finished(reportOversizedResponder);
      } finally {
        rPart1.release();
        rPart2.release();
        rPart3.release();
      }
      Assert.assertEquals(4, downstreamCalls.get());
      Assert.assertEquals(largeExtBody, capturedDatasetBody.get());
      Assert.assertEquals(Long.valueOf(2L),
          reportCounters.get(RuntimeValidationFailure.API_NOT_ALLOWED.getMetricName()));
      Mockito.verify(reportOversizedResponder).sendContent(Mockito.eq(HttpResponseStatus.OK),
          Mockito.any(BodyProducer.class), Mockito.any(HttpHeaders.class));
    } finally {
      datasetReg.cancel();
      datasetService.stop();
    }
  }

  @Test
  public void testDatasetPutBufferingErrorHandling() throws Exception {
    ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf")
        .run(RunIds.generate());
    RuntimeRequestValidator runtimeValidator = (id, req) ->
        ValidatedRuntimeRequest.runtimeToken(
            new ProgramRunInfo(ProgramRunStatus.RUNNING), runId, null);
    InternalAuthenticator stubAuthenticator =
        headerSetter -> headerSetter.accept(Constants.Security.Headers.RUNTIME_TOKEN,
            "CDAP-Internal exchanged");

    AtomicInteger downstreamCalls = new AtomicInteger();
    AtomicReference<String> capturedDatasetBody = new AtomicReference<>();
    AtomicReference<Map<String, String>> capturedDatasetHeaders = new AtomicReference<>();
    NettyHttpService datasetService = NettyHttpService.builder(Constants.Service.DATASET_MANAGER)
        .setHost(InetAddress.getLocalHost().getCanonicalHostName())
        .setHttpHandlers(new DatasetCaptureHandler(
            downstreamCalls, capturedDatasetBody, capturedDatasetHeaders))
        .build();
    datasetService.start();
    Cancellable datasetReg = injector.getInstance(DiscoveryService.class)
        .register(URIScheme.createDiscoverable(Constants.Service.DATASET_MANAGER, datasetService));

    try {
      String basePrefix = String.format(
          "%s/runtime/namespaces/%s/apps/%s/versions/%s/%s/%s/runs/%s/services/%s",
          Constants.Gateway.INTERNAL_API_VERSION_3,
          runId.getNamespace(), runId.getApplication(), runId.getVersion(),
          runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
          Constants.Service.DATASET_MANAGER);
      RuntimeCallbackAllowlist allowlist = new RuntimeCallbackAllowlist(CConfiguration.create());
      RuntimeServiceRoutingHandler enforcingHandler = new RuntimeServiceRoutingHandler(
          injector.getInstance(DiscoveryServiceClient.class),
          runtimeValidator,
          stubAuthenticator,
          allowlist,
          createReporter(true, new HashMap<>(), Mockito.mock(Logger.class)),
          128);
      String extUri = basePrefix + "/v3/namespaces/default/data/datasets/ext-ds";
      DefaultHttpRequest extReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
          io.netty.handler.codec.http.HttpMethod.PUT, extUri);

      // 1. Empty body (0-byte chunk and no chunks): enforcing -> 403; report-only -> forwarded
      HttpResponder emptyEnforceResp = mockResponder();
      BodyConsumer emptyEnforceConsumer = routeWithBody(
          enforcingHandler, extReq, emptyEnforceResp, runId, Constants.Service.DATASET_MANAGER);
      emptyEnforceConsumer.chunk(Unpooled.EMPTY_BUFFER, emptyEnforceResp);
      emptyEnforceConsumer.finished(emptyEnforceResp);
      Assert.assertEquals(0, downstreamCalls.get());
      Mockito.verify(emptyEnforceResp).sendString(
          Mockito.eq(HttpResponseStatus.FORBIDDEN), Mockito.anyString());

      Map<String, Long> reportCounters = new HashMap<>();
      RuntimeServiceRoutingHandler reportHandler = new RuntimeServiceRoutingHandler(
          injector.getInstance(DiscoveryServiceClient.class),
          runtimeValidator,
          stubAuthenticator,
          allowlist,
          createReporter(false, reportCounters, Mockito.mock(Logger.class)),
          128);
      HttpResponder emptyReportResp = mockResponder();
      BodyConsumer emptyReportConsumer = routeWithBody(
          reportHandler, extReq, emptyReportResp, runId, Constants.Service.DATASET_MANAGER);
      emptyReportConsumer.finished(emptyReportResp);
      Assert.assertEquals(1, downstreamCalls.get());
      Assert.assertEquals("", capturedDatasetBody.get());
      Assert.assertEquals(Long.valueOf(1L),
          reportCounters.get(RuntimeValidationFailure.API_NOT_ALLOWED.getMetricName()));

      // 2. Downstream HTTP 400 relayed through BufferingBodyConsumer, and BadRequestException /
      //    ServiceException from downstream handled via responder with buffer released
      DefaultHttpRequest bad400Req = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
          io.netty.handler.codec.http.HttpMethod.PUT,
          basePrefix + "/v3/namespaces/default/data/datasets/return-400");
      HttpResponder relay400Resp = mockResponder();
      BodyConsumer relay400Consumer = routeWithBody(
          enforcingHandler, bad400Req, relay400Resp, runId, Constants.Service.DATASET_MANAGER);
      String extBody = GSON.toJson(new DatasetInstanceConfiguration(
          ExternalDataset.TYPE,
          Collections.singletonMap(FileSetProperties.BASE_PATH, "/data/ext")));
      sendSingleChunkAndFinish(relay400Consumer, relay400Resp, extBody);
      Mockito.verify(relay400Resp).sendContent(Mockito.eq(HttpResponseStatus.BAD_REQUEST),
          Mockito.any(BodyProducer.class), Mockito.any(HttpHeaders.class));

      RuntimeServiceRoutingHandler badReqDownstreamHandler = new RuntimeServiceRoutingHandler(
          injector.getInstance(DiscoveryServiceClient.class),
          runtimeValidator,
          stubAuthenticator,
          allowlist,
          createReporter(true, new HashMap<>(), Mockito.mock(Logger.class)),
          128) {
        @Override
        public BodyConsumer routeServiceWithBody(HttpRequest request, HttpResponder responder,
            String service, String path) throws Exception {
          throw new BadRequestException("bad downstream request");
        }
      };
      HttpResponder badOpenResp = mockResponder();
      BodyConsumer badOpenConsumer = routeWithBody(
          badReqDownstreamHandler, extReq, badOpenResp, runId, Constants.Service.DATASET_MANAGER);
      sendSingleChunkAndFinish(badOpenConsumer, badOpenResp, extBody);
      Mockito.verify(badOpenResp).sendString(
          Mockito.eq(HttpResponseStatus.BAD_REQUEST), Mockito.eq("bad downstream request"));

      RuntimeServiceRoutingHandler svcExDownstreamHandler = new RuntimeServiceRoutingHandler(
          injector.getInstance(DiscoveryServiceClient.class),
          runtimeValidator,
          stubAuthenticator,
          allowlist,
          createReporter(true, new HashMap<>(), Mockito.mock(Logger.class)),
          128) {
        @Override
        public BodyConsumer routeServiceWithBody(HttpRequest request, HttpResponder responder,
            String service, String path) {
          return new BodyConsumer() {
            @Override
            public void chunk(ByteBuf request, HttpResponder responder) {
            }

            @Override
            public void finished(HttpResponder responder) {
              throw new ServiceException("downstream rejected", null,
                  HttpResponseStatus.BAD_REQUEST);
            }

            @Override
            public void handleError(Throwable cause) {
            }
          };
        }
      };
      HttpResponder svcExResp = mockResponder();
      BodyConsumer svcExConsumer = routeWithBody(
          svcExDownstreamHandler, extReq, svcExResp, runId, Constants.Service.DATASET_MANAGER);
      sendSingleChunkAndFinish(svcExConsumer, svcExResp, extBody);
      Mockito.verify(svcExResp).sendString(
          Mockito.eq(HttpResponseStatus.BAD_REQUEST), Mockito.eq("downstream rejected"));

      IllegalStateException internalFailure =
          new IllegalStateException("internal sensitive detail");
      AtomicInteger handleErrorCalls = new AtomicInteger();
      AtomicReference<Throwable> handleErrorCause = new AtomicReference<>();
      RuntimeServiceRoutingHandler runtimeExDownstreamHandler = new RuntimeServiceRoutingHandler(
          injector.getInstance(DiscoveryServiceClient.class),
          runtimeValidator,
          stubAuthenticator,
          allowlist,
          createReporter(true, new HashMap<>(), Mockito.mock(Logger.class)),
          128) {
        @Override
        public BodyConsumer routeServiceWithBody(HttpRequest request, HttpResponder responder,
            String service, String path) {
          return new BodyConsumer() {
            @Override
            public void chunk(ByteBuf request, HttpResponder responder) {
            }

            @Override
            public void finished(HttpResponder responder) {
              throw internalFailure;
            }

            @Override
            public void handleError(Throwable cause) {
              handleErrorCalls.incrementAndGet();
              handleErrorCause.set(cause);
            }
          };
        }
      };
      HttpResponder runtimeExResp = mockResponder();
      AtomicReference<String> runtimeExBody = new AtomicReference<>();
      Mockito.doAnswer(invocation -> {
        runtimeExBody.set((String) invocation.getArguments()[1]);
        return null;
      }).when(runtimeExResp).sendString(Mockito.any(HttpResponseStatus.class), Mockito.anyString());
      BodyConsumer runtimeExConsumer = routeWithBody(
          runtimeExDownstreamHandler, extReq, runtimeExResp, runId,
          Constants.Service.DATASET_MANAGER);
      sendSingleChunkAndFinish(runtimeExConsumer, runtimeExResp, extBody);
      Mockito.verify(runtimeExResp).sendString(
          Mockito.eq(HttpResponseStatus.INTERNAL_SERVER_ERROR),
          Mockito.eq("Failed to forward callback request"));
      Assert.assertEquals("Failed to forward callback request", runtimeExBody.get());
      Assert.assertFalse(runtimeExBody.get().contains("internal sensitive detail"));
      Assert.assertEquals(1, handleErrorCalls.get());
      Assert.assertSame(internalFailure, handleErrorCause.get());

      // 3. Buffer released on handleError (and repeated handleError is idempotent)
      HttpResponder errResponder = mockResponder();
      BodyConsumer errConsumer = routeWithBody(
          enforcingHandler, extReq, errResponder, runId, Constants.Service.DATASET_MANAGER);
      ByteBuf errBuf = Unpooled.copiedBuffer("partial", StandardCharsets.UTF_8);
      try {
        errConsumer.chunk(errBuf, errResponder);
        Assert.assertEquals(2, errBuf.refCnt());
        errConsumer.handleError(new IOException("client disconnected"));
        Assert.assertEquals(1, errBuf.refCnt());
        errConsumer.handleError(new IOException("duplicate error"));
        Assert.assertEquals(1, errBuf.refCnt());
      } finally {
        errBuf.release();
      }
    } finally {
      datasetReg.cancel();
      datasetService.stop();
    }

    // 4. Downstream unavailable -> 503 via responder and buffer released
    String unavailUri = String.format(
        "%s/runtime/namespaces/%s/apps/%s/versions/%s/%s/%s/runs/%s/services/%s"
            + "/v3/namespaces/default/data/datasets/ext-ds",
        Constants.Gateway.INTERNAL_API_VERSION_3,
        runId.getNamespace(), runId.getApplication(), runId.getVersion(),
        runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
        Constants.Service.DATASET_MANAGER);
    RuntimeServiceRoutingHandler unavailHandler = new RuntimeServiceRoutingHandler(
        injector.getInstance(DiscoveryServiceClient.class),
        runtimeValidator,
        stubAuthenticator,
        new RuntimeCallbackAllowlist(CConfiguration.create()),
        createReporter(true, new HashMap<>(), Mockito.mock(Logger.class)),
        128);
    DefaultHttpRequest unavailReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
        io.netty.handler.codec.http.HttpMethod.PUT, unavailUri);
    HttpResponder unavailResponder = mockResponder();
    BodyConsumer unavailConsumer = routeWithBody(
        unavailHandler, unavailReq, unavailResponder, runId, Constants.Service.DATASET_MANAGER);
    String validBody = GSON.toJson(new DatasetInstanceConfiguration(
        ExternalDataset.TYPE, Collections.emptyMap()));
    ByteBuf unavailBuf = Unpooled.copiedBuffer(validBody, StandardCharsets.UTF_8);
    try {
      unavailConsumer.chunk(unavailBuf, unavailResponder);
      Assert.assertEquals(2, unavailBuf.refCnt());
      unavailConsumer.finished(unavailResponder);
      Assert.assertEquals(1, unavailBuf.refCnt());
    } finally {
      unavailBuf.release();
    }
    Mockito.verify(unavailResponder).sendString(
        Mockito.eq(HttpResponseStatus.SERVICE_UNAVAILABLE), Mockito.anyString());
  }

  @Test
  public void testInvalidOuterPrefixRejectedWithBadRequest() throws Exception {
    ProgramRunId runId = new NamespaceId("runns").app("myApp", "1.0").workflow("wf")
        .run(RunIds.generate());
    String service = Constants.Service.SECURE_STORE_SERVICE;
    String validPrefix = String.format(
        "%s/runtime/namespaces/%s/apps/%s/versions/%s/%s/%s/runs/%s/services/%s",
        Constants.Gateway.INTERNAL_API_VERSION_3,
        runId.getNamespace(), runId.getApplication(), runId.getVersion(),
        runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), service);
    final String[] badUris = new String[] {
        "http://localhost:11015" + validPrefix + "/v3/namespaces/runns/securekeys/my-key",
        validPrefix.replace("/apps/myApp/", "/apps/my%41pp/")
            + "/v3/namespaces/runns/securekeys/my-key"
    };

    RuntimeRequestValidator legacyValidator = (id, req) ->
        ValidatedRuntimeRequest.legacy(new ProgramRunInfo(ProgramRunStatus.RUNNING));
    RuntimeRequestValidator runtimeValidator = (id, req) ->
        ValidatedRuntimeRequest.runtimeToken(
            new ProgramRunInfo(ProgramRunStatus.RUNNING), runId, null);
    InternalAuthenticator stubAuthenticator =
        headerSetter -> headerSetter.accept(Constants.Security.Headers.USER_ID, "system");
    RuntimeCallbackAllowlist allowlist = new RuntimeCallbackAllowlist(CConfiguration.create());

    for (boolean isRuntime : Arrays.asList(false, true)) {
      for (boolean enforcing : isRuntime ? Arrays.asList(true, false) : Arrays.asList(true)) {
        Map<String, Long> counters = new HashMap<>();
        AtomicInteger forwarded = new AtomicInteger();
        RuntimeServiceRoutingHandler handler = new RuntimeServiceRoutingHandler(
            injector.getInstance(DiscoveryServiceClient.class),
            isRuntime ? runtimeValidator : legacyValidator,
            stubAuthenticator,
            allowlist,
            createReporter(enforcing, counters, Mockito.mock(Logger.class))) {
          @Override
          public void routeService(HttpRequest request, HttpResponder responder,
              String targetService, String path) {
            forwarded.incrementAndGet();
          }
        };

        for (String badUri : badUris) {
          DefaultHttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
              io.netty.handler.codec.http.HttpMethod.GET, badUri);
          try {
            handler.routeService(req, mockResponder(),
                runId.getNamespace(), runId.getApplication(), runId.getVersion(),
                runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), service);
            Assert.fail("Expected BadRequestException for " + badUri);
          } catch (BadRequestException expected) {
            // expected
          }
        }
        Assert.assertEquals(0, forwarded.get());
        Assert.assertTrue(counters.isEmpty());
      }
    }
  }

  @Test
  public void testReportOnlyStreamingOverflowErrors() throws Exception {
    ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf")
        .run(RunIds.generate());
    RuntimeRequestValidator runtimeValidator = (id, req) ->
        ValidatedRuntimeRequest.runtimeToken(
            new ProgramRunInfo(ProgramRunStatus.RUNNING), runId, null);
    InternalAuthenticator stubAuthenticator =
        headerSetter -> headerSetter.accept(Constants.Security.Headers.RUNTIME_TOKEN,
            "CDAP-Internal exchanged");
    RuntimeCallbackAllowlist allowlist = new RuntimeCallbackAllowlist(CConfiguration.create());
    String dsUri = String.format(
        "%s/runtime/namespaces/%s/apps/%s/versions/%s/%s/%s/runs/%s/services/%s"
            + "/v3/namespaces/default/data/datasets/ext-ds",
        Constants.Gateway.INTERNAL_API_VERSION_3,
        runId.getNamespace(), runId.getApplication(), runId.getVersion(),
        runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
        Constants.Service.DATASET_MANAGER);
    DefaultHttpRequest putReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
        io.netty.handler.codec.http.HttpMethod.PUT, dsUri);

    // 1. Downstream open fails on overflow (503): buffer released, trailing chunk ignored,
    //    counted once, finished() answers 503
    Map<String, Long> openFailCounters = new HashMap<>();
    RuntimeServiceRoutingHandler openFailHandler = new RuntimeServiceRoutingHandler(
        injector.getInstance(DiscoveryServiceClient.class),
        runtimeValidator,
        stubAuthenticator,
        allowlist,
        createReporter(false, openFailCounters, Mockito.mock(Logger.class)),
        64) {
      @Override
      public BodyConsumer routeServiceWithBody(HttpRequest request, HttpResponder responder,
          String service, String path) {
        throw new ServiceUnavailableException(service);
      }
    };
    HttpResponder openFailResponder = mockResponder();
    BodyConsumer openFailConsumer = openFailHandler.routeServiceWithBody(
        putReq, openFailResponder,
        runId.getNamespace(), runId.getApplication(), runId.getVersion(),
        runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
        Constants.Service.DATASET_MANAGER);
    ByteBuf c1 = Unpooled.copiedBuffer(Strings.repeat("a", 40), StandardCharsets.UTF_8);
    ByteBuf c2 = Unpooled.copiedBuffer(Strings.repeat("b", 40), StandardCharsets.UTF_8);
    ByteBuf c3 = Unpooled.copiedBuffer(Strings.repeat("c", 20), StandardCharsets.UTF_8);
    try {
      openFailConsumer.chunk(c1, openFailResponder);
      Assert.assertEquals(2, c1.refCnt());
      openFailConsumer.chunk(c2, openFailResponder);
      Assert.assertEquals(1, c1.refCnt());
      Assert.assertEquals(1, c2.refCnt());
      openFailConsumer.chunk(c3, openFailResponder);
      Assert.assertEquals(1, c3.refCnt());
      openFailConsumer.finished(openFailResponder);
    } finally {
      c1.release();
      c2.release();
      c3.release();
    }
    Assert.assertEquals(Long.valueOf(1L),
        openFailCounters.get(RuntimeValidationFailure.API_NOT_ALLOWED.getMetricName()));
    Mockito.verify(openFailResponder).sendString(
        Mockito.eq(HttpResponseStatus.SERVICE_UNAVAILABLE), Mockito.anyString());

    // 2. Chunk relay fails after streaming starts: downstream.handleError called once,
    //    downstream.finished never called, finished() sends recorded status, subsequent
    //    handleError does not double-invoke downstream.handleError
    AtomicInteger downstreamErrors = new AtomicInteger();
    AtomicInteger downstreamFinished = new AtomicInteger();
    AtomicInteger downstreamChunks = new AtomicInteger();
    RuntimeServiceRoutingHandler chunkFailHandler = new RuntimeServiceRoutingHandler(
        injector.getInstance(DiscoveryServiceClient.class),
        runtimeValidator,
        stubAuthenticator,
        allowlist,
        createReporter(false, new HashMap<>(), Mockito.mock(Logger.class)),
        64) {
      @Override
      public BodyConsumer routeServiceWithBody(HttpRequest request, HttpResponder responder,
          String service, String path) {
        return new BodyConsumer() {
          @Override
          public void chunk(ByteBuf request, HttpResponder responder) {
            if (downstreamChunks.incrementAndGet() == 3) {
              throw new ServiceUnavailableException(service);
            }
          }

          @Override
          public void finished(HttpResponder responder) {
            downstreamFinished.incrementAndGet();
          }

          @Override
          public void handleError(Throwable cause) {
            downstreamErrors.incrementAndGet();
          }
        };
      }
    };
    HttpResponder chunkFailResponder = mockResponder();
    BodyConsumer chunkFailConsumer = chunkFailHandler.routeServiceWithBody(
        putReq, chunkFailResponder,
        runId.getNamespace(), runId.getApplication(), runId.getVersion(),
        runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
        Constants.Service.DATASET_MANAGER);
    ByteBuf s1 = Unpooled.copiedBuffer(Strings.repeat("a", 40), StandardCharsets.UTF_8);
    ByteBuf s2 = Unpooled.copiedBuffer(Strings.repeat("b", 40), StandardCharsets.UTF_8);
    ByteBuf s3 = Unpooled.copiedBuffer(Strings.repeat("c", 20), StandardCharsets.UTF_8);
    ByteBuf s4 = Unpooled.copiedBuffer(Strings.repeat("d", 10), StandardCharsets.UTF_8);
    try {
      chunkFailConsumer.chunk(s1, chunkFailResponder);
      chunkFailConsumer.chunk(s2, chunkFailResponder);
      chunkFailConsumer.chunk(s3, chunkFailResponder);
      chunkFailConsumer.chunk(s4, chunkFailResponder);
      chunkFailConsumer.finished(chunkFailResponder);
      chunkFailConsumer.handleError(new IOException("late error"));
    } finally {
      s1.release();
      s2.release();
      s3.release();
      s4.release();
    }
    Assert.assertEquals(1, downstreamErrors.get());
    Assert.assertEquals(0, downstreamFinished.get());
    Mockito.verify(chunkFailResponder).sendString(
        Mockito.eq(HttpResponseStatus.SERVICE_UNAVAILABLE), Mockito.anyString());

    // 3. Client disconnect during active streaming forwards handleError once to downstream
    AtomicInteger activeStreamErrors = new AtomicInteger();
    RuntimeServiceRoutingHandler activeStreamHandler = new RuntimeServiceRoutingHandler(
        injector.getInstance(DiscoveryServiceClient.class),
        runtimeValidator,
        stubAuthenticator,
        allowlist,
        createReporter(false, new HashMap<>(), Mockito.mock(Logger.class)),
        64) {
      @Override
      public BodyConsumer routeServiceWithBody(HttpRequest request, HttpResponder responder,
          String service, String path) {
        return new BodyConsumer() {
          @Override
          public void chunk(ByteBuf request, HttpResponder responder) {
          }

          @Override
          public void finished(HttpResponder responder) {
          }

          @Override
          public void handleError(Throwable cause) {
            activeStreamErrors.incrementAndGet();
          }
        };
      }
    };
    HttpResponder activeResponder = mockResponder();
    BodyConsumer activeConsumer = activeStreamHandler.routeServiceWithBody(
        putReq, activeResponder,
        runId.getNamespace(), runId.getApplication(), runId.getVersion(),
        runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
        Constants.Service.DATASET_MANAGER);
    ByteBuf a1 = Unpooled.copiedBuffer(Strings.repeat("a", 40), StandardCharsets.UTF_8);
    ByteBuf a2 = Unpooled.copiedBuffer(Strings.repeat("b", 40), StandardCharsets.UTF_8);
    try {
      activeConsumer.chunk(a1, activeResponder);
      activeConsumer.chunk(a2, activeResponder);
      activeConsumer.handleError(new IOException("client disconnected mid-stream"));
      activeConsumer.handleError(new IOException("duplicate"));
    } finally {
      a1.release();
      a2.release();
    }
    Assert.assertEquals(1, activeStreamErrors.get());
  }

  @Test
  public void testRuntimeServerHttpAllowlistEndToEnd() throws Exception {
    ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf")
        .run(RunIds.generate());
    RuntimeRequestValidator runtimeValidator = (id, req) ->
        ValidatedRuntimeRequest.runtimeToken(
            new ProgramRunInfo(ProgramRunStatus.RUNNING), runId, null);
    InternalAuthenticator stubAuthenticator =
        headerSetter -> headerSetter.accept(Constants.Security.Headers.RUNTIME_TOKEN,
            "CDAP-Internal exchanged");

    AtomicInteger datasetCalls = new AtomicInteger();
    AtomicReference<String> capturedDatasetBody = new AtomicReference<>();
    AtomicReference<Map<String, String>> capturedDatasetHeaders = new AtomicReference<>();
    NettyHttpService datasetService = NettyHttpService.builder(Constants.Service.DATASET_MANAGER)
        .setHost(InetAddress.getLocalHost().getCanonicalHostName())
        .setHttpHandlers(new DatasetCaptureHandler(
            datasetCalls, capturedDatasetBody, capturedDatasetHeaders))
        .build();
    datasetService.start();
    Cancellable datasetReg = injector.getInstance(DiscoveryService.class)
        .register(URIScheme.createDiscoverable(Constants.Service.DATASET_MANAGER, datasetService));
    NettyHttpService enforcingServer = null;

    try {
      RuntimeServiceRoutingHandler routingHandler = new RuntimeServiceRoutingHandler(
          injector.getInstance(DiscoveryServiceClient.class),
          runtimeValidator,
          stubAuthenticator,
          new RuntimeCallbackAllowlist(CConfiguration.create()),
          createReporter(true, new HashMap<>(), Mockito.mock(Logger.class)));
      enforcingServer = NettyHttpService.builder("runtime-allowlist-e2e")
          .setHost(InetAddress.getLocalHost().getCanonicalHostName())
          .setHttpHandlers(routingHandler)
          .setExceptionHandler(new RuntimeServer.RuntimeHttpExceptionHandler())
          .build();
      enforcingServer.start();

      String base = "http://" + enforcingServer.getBindAddress().getHostName() + ":"
          + enforcingServer.getBindAddress().getPort()
          + String.format("%s/runtime/namespaces/%s/apps/%s/versions/%s/%s/%s/runs/%s/services/",
          Constants.Gateway.INTERNAL_API_VERSION_3,
          runId.getNamespace(), runId.getApplication(), runId.getVersion(),
          runId.getType().getCategoryName(), runId.getProgram(), runId.getRun());

      // 1. Disallowed GET maps UnauthorizedException to HTTP 403 without token failure header
      HttpURLConnection disallowedGet = (HttpURLConnection) new URL(
          base + Constants.Service.APP_FABRIC_HTTP + "/v3/namespaces/default/apps")
          .openConnection();
      try {
        disallowedGet.setRequestMethod("GET");
        Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, disallowedGet.getResponseCode());
        Assert.assertNull(
            disallowedGet.getHeaderField(Constants.Security.Headers.RUNTIME_TOKEN_FAILURE));
      } finally {
        disallowedGet.disconnect();
      }

      // 2. Disallowed POST with body maps pre-body UnauthorizedException to HTTP 403 over wire
      HttpURLConnection disallowedPost = (HttpURLConnection) new URL(
          base + Constants.Service.APP_FABRIC_HTTP + "/v3/namespaces/default/apps/app/start")
          .openConnection();
      try {
        disallowedPost.setRequestMethod("POST");
        disallowedPost.setDoOutput(true);
        try (OutputStream os = disallowedPost.getOutputStream()) {
          os.write("{\"arg\":\"val\"}".getBytes(StandardCharsets.UTF_8));
        }
        Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, disallowedPost.getResponseCode());
        Assert.assertNull(
            disallowedPost.getHeaderField(Constants.Security.Headers.RUNTIME_TOKEN_FAILURE));
      } finally {
        disallowedPost.disconnect();
      }

      // 3. Allowed externalDataset PUT streams through NettyHttpService -> BufferingBodyConsumer
      String extBody = GSON.toJson(new DatasetInstanceConfiguration(
          ExternalDataset.TYPE, Collections.singletonMap("base.path", "/ext/data")));
      HttpURLConnection allowedPut = (HttpURLConnection) new URL(
          base + Constants.Service.DATASET_MANAGER
              + "/v3/namespaces/default/data/datasets/e2e-ext")
          .openConnection();
      try {
        allowedPut.setRequestMethod("PUT");
        allowedPut.setDoOutput(true);
        try (OutputStream os = allowedPut.getOutputStream()) {
          os.write(extBody.getBytes(StandardCharsets.UTF_8));
        }
        Assert.assertEquals(HttpURLConnection.HTTP_OK, allowedPut.getResponseCode());
      } finally {
        allowedPut.disconnect();
      }
      Assert.assertEquals(1, datasetCalls.get());
      Assert.assertEquals(extBody, capturedDatasetBody.get());

      // 4. Forbidden dataset PUT returns HTTP 403 over the wire without calling downstream
      String badBody = GSON.toJson(new DatasetInstanceConfiguration(
          FileSet.TYPE, Collections.singletonMap(FileSetProperties.BASE_PATH, "/etc")));
      HttpURLConnection forbiddenPut = (HttpURLConnection) new URL(
          base + Constants.Service.DATASET_MANAGER
              + "/v3/namespaces/default/data/datasets/"
              + RuntimeCallbackAllowlist.DEFAULT_CHECKPOINT_DATASET)
          .openConnection();
      try {
        forbiddenPut.setRequestMethod("PUT");
        forbiddenPut.setDoOutput(true);
        try (OutputStream os = forbiddenPut.getOutputStream()) {
          os.write(badBody.getBytes(StandardCharsets.UTF_8));
        }
        Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, forbiddenPut.getResponseCode());
        Assert.assertNull(
            forbiddenPut.getHeaderField(Constants.Security.Headers.RUNTIME_TOKEN_FAILURE));
      } finally {
        forbiddenPut.disconnect();
      }
      Assert.assertEquals(1, datasetCalls.get());
    } finally {
      if (enforcingServer != null) {
        enforcingServer.stop();
      }
      datasetReg.cancel();
      datasetService.stop();
    }
  }

  private static BodyConsumer routeWithBody(RuntimeServiceRoutingHandler handler,
      HttpRequest request, HttpResponder responder, ProgramRunId runId, String service)
      throws Exception {
    return handler.routeServiceWithBody(request, responder,
        runId.getNamespace(), runId.getApplication(), runId.getVersion(),
        runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), service);
  }

  private static void sendSingleChunkAndFinish(BodyConsumer consumer, HttpResponder responder,
      String body) {
    ByteBuf buf = Unpooled.copiedBuffer(body, StandardCharsets.UTF_8);
    try {
      consumer.chunk(buf, responder);
      consumer.finished(responder);
      Assert.assertEquals(1, buf.refCnt());
    } finally {
      buf.release();
    }
  }

  private static HttpResponder mockResponder() {
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    Mockito.doAnswer(invocation -> {
      BodyProducer producer = (BodyProducer) invocation.getArguments()[1];
      if (producer != null) {
        try {
          ByteBuf chunk;
          while ((chunk = producer.nextChunk()).isReadable()) {
            chunk.release();
          }
          producer.finished();
        } catch (Exception e) {
          producer.handleError(e);
          throw e;
        }
      }
      return null;
    }).when(responder).sendContent(Mockito.any(HttpResponseStatus.class),
        Mockito.any(BodyProducer.class), Mockito.any(HttpHeaders.class));
    return responder;
  }

  private static RuntimeValidationReporter createReporter(boolean enforcing,
      Map<String, Long> counters, Logger logger) {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.RuntimeMonitor.RUNTIME_TOKEN_VALIDATION_ENABLED, enforcing);
    MetricsContext metricsContext = Mockito.mock(MetricsContext.class);
    Mockito.doAnswer(invocation -> {
      String name = (String) invocation.getArguments()[0];
      long delta = (Long) invocation.getArguments()[1];
      counters.merge(name, delta, Long::sum);
      return null;
    }).when(metricsContext).increment(Mockito.anyString(), Mockito.anyLong());
    MetricsCollectionService metricsService = Mockito.mock(MetricsCollectionService.class);
    Mockito.when(metricsService.getContext(Mockito.anyMap()))
        .thenReturn(metricsContext);
    return new RuntimeValidationReporter(cConf, metricsService, logger, TimeProvider.SYSTEM_TIME);
  }

  private static Map<String, String> copyHeaders(HttpHeaders headers) {
    Map<String, String> map = new HashMap<>();
    for (Map.Entry<String, String> entry : headers.entries()) {
      map.put(entry.getKey().toLowerCase(), entry.getValue());
    }
    return map;
  }

  /**
   * Public HTTP handler that captures forwarded headers and request body for assertions.
   */
  public static final class HeaderCaptureHandler extends AbstractHttpHandler {

    private final AtomicReference<Map<String, String>> capturedHeaders;
    private final AtomicReference<String> capturedBody;

    public HeaderCaptureHandler(AtomicReference<Map<String, String>> capturedHeaders,
        AtomicReference<String> capturedBody) {
      this.capturedHeaders = capturedHeaders;
      this.capturedBody = capturedBody;
    }

    @Path("/echo")
    @GET
    public void echoGet(HttpRequest req, HttpResponder responder) {
      capturedHeaders.set(copyHeaders(req.headers()));
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @Path("/echo")
    @POST
    public void echoPost(FullHttpRequest req, HttpResponder responder) {
      capturedHeaders.set(copyHeaders(req.headers()));
      capturedBody.set(req.content().toString(StandardCharsets.UTF_8));
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @Path("/echo path")
    @GET
    public void echoSpaceGet(HttpRequest req, HttpResponder responder) {
      capturedHeaders.set(copyHeaders(req.headers()));
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @Path("/echo path")
    @POST
    public void echoSpacePost(FullHttpRequest req, HttpResponder responder) {
      capturedHeaders.set(copyHeaders(req.headers()));
      capturedBody.set(req.content().toString(StandardCharsets.UTF_8));
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  /**
   * Public HTTP handler simulating dataset endpoints to verify buffered PUT and GET/DELETE routing.
   */
  public static final class DatasetCaptureHandler extends AbstractHttpHandler {

    private final AtomicInteger calls;
    private final AtomicReference<String> capturedBody;
    private final AtomicReference<Map<String, String>> capturedHeaders;

    public DatasetCaptureHandler(AtomicInteger calls, AtomicReference<String> capturedBody,
        AtomicReference<Map<String, String>> capturedHeaders) {
      this.calls = calls;
      this.capturedBody = capturedBody;
      this.capturedHeaders = capturedHeaders;
    }

    @GET
    @DELETE
    @Path("/v3/namespaces/{namespace}/data/datasets/{name}")
    public void getOrDeleteDataset(HttpRequest req, HttpResponder responder,
        @PathParam("namespace") String namespace, @PathParam("name") String name) {
      calls.incrementAndGet();
      capturedHeaders.set(copyHeaders(req.headers()));
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @PUT
    @Path("/v3/namespaces/{namespace}/data/datasets/{name}")
    public void createDataset(FullHttpRequest req, HttpResponder responder,
        @PathParam("namespace") String namespace, @PathParam("name") String name) {
      calls.incrementAndGet();
      capturedHeaders.set(copyHeaders(req.headers()));
      capturedBody.set(req.content().toString(StandardCharsets.UTF_8));
      if ("return-400".equals(name)) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "bad dataset");
        return;
      }
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  /**
   * A {@link RemoteAuthenticator} for testing. It generates a md5 signature from the program run id.
   */
  private static final class MockRemoteAuthenticator implements RemoteAuthenticator {

    private final ProgramRunId programRunId;

    private MockRemoteAuthenticator(ProgramRunId programRunId) {
      this.programRunId = programRunId;
    }

    @Override
    public String getName() {
      return "mock-remote-authenticator";
    }

    @Override
    public Credential getCredentials() {
      String credentialValue = Base64.getEncoder().encodeToString(
          Hashing.md5().hashUnencodedChars(programRunId.toString()).asBytes());
      return new Credential(credentialValue, Credential.CredentialType.EXTERNAL_BEARER);
    }

    /**
     * Returns the credentials for the authentication with scopes.
     */
    @Nullable
    @Override
    public Credential getCredentials(String scopes) throws IOException {
      return getCredentials();
    }
  }

  /**
   * A {@link RemoteAuthenticator} provider for testing.
   */
  private static final class MockRemoteAuthenticatorProvider implements Provider<RemoteAuthenticator> {

    private RemoteAuthenticator remoteAuthenticator = new NoOpRemoteAuthenticator();

    public void setAuthenticator(RemoteAuthenticator remoteAuthenticator) {
      this.remoteAuthenticator = remoteAuthenticator;
    }

    @Override
    public RemoteAuthenticator get() {
      return remoteAuthenticator;
    }
  }
}
