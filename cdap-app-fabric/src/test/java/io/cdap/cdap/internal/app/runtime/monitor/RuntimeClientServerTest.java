/*
 * Copyright © 2020-2022 Cask Data, Inc.
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
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.RuntimeServerModule;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.encryption.guice.UserCredentialAeadEncryptionModule;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.NoOpAuditLogModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.NoOpInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.service.Services;
import io.cdap.cdap.internal.MockResponder;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Unit test for {@link RuntimeServer} and {@link RuntimeClient}.
 */
@RunWith(Parameterized.class)
public class RuntimeClientServerTest {

  public static final String TEST_TOPIC_KEY = "topic.key";
  public static final String TEST_TOPIC = "topic";

  @Parameterized.Parameters(name = "{index}: compression = {0}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][]{
      {false},
      {true},
    });
  }

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private final List<String> logEntries = new ArrayList<>();
  private final boolean compression;
  private CConfiguration cConf;
  private MessagingService messagingService;
  private RuntimeServer runtimeServer;
  private RuntimeClient runtimeClient;
  private RemoteClientFactory remoteClientFactory;
  private LocationFactory locationFactory;
  private AtomicReference<RuntimeRequestValidator> requestValidatorRef;

  public RuntimeClientServerTest(boolean compression) {
    this.compression = compression;
  }

  @Before
  public void beforeTest() throws Exception {
    requestValidatorRef = new AtomicReference<>(
        (programRunId, request) -> ValidatedRuntimeRequest.legacy(
            new ProgramRunInfo(Long.MAX_VALUE)));
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.RuntimeMonitor.COMPRESSION_ENABLED, compression);
    cConf.setBoolean(Constants.AppFabric.SPARK_EVENT_LOGS_ENABLED, true);
    cConf.set(TEST_TOPIC_KEY, TEST_TOPIC);
    cConf.set(Constants.RuntimeMonitor.TOPICS_CONFIGS, Constants.Logging.TMS_TOPIC_PREFIX + ":1," + TEST_TOPIC_KEY);

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      RemoteAuthenticatorModules.getNoOpModule(),
      new NoOpAuditLogModule(),
      new UserCredentialAeadEncryptionModule(),
      new InMemoryDiscoveryModule(),
      new LocalLocationModule(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new AuthenticationContextModules().getNoOpModule(),
      new AuthorizationEnforcementModule().getNoOpModules(),
      new RuntimeServerModule() {
        @Override
        protected void bindRequestValidator() {
          bind(RuntimeRequestValidator.class).toInstance(
              (programRunId, request) -> requestValidatorRef.get().validate(programRunId, request));
        }

        @Override
        protected void bindLogProcessor() {
          bind(RemoteExecutionLogProcessor.class).toInstance(payloads -> {
            // For testing purpose, we just store logs to a list
            payloads.forEachRemaining(bytes -> logEntries.add(new String(bytes, StandardCharsets.UTF_8)));
          });
        }
      },
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
        }
      }
    );

    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      Services.startAndWait((Service) messagingService);
    }
    messagingService.createTopic(new DefaultTopicMetadata(NamespaceId.SYSTEM.topic("topic")));

    runtimeServer = injector.getInstance(RuntimeServer.class);
    Services.startAndWait(runtimeServer);

    runtimeClient = injector.getInstance(RuntimeClient.class);
    remoteClientFactory = injector.getInstance(RemoteClientFactory.class);
    locationFactory = injector.getInstance(LocationFactory.class);
  }

  @After
  public void afterTest() {
    logEntries.clear();
    Services.stopAndWait(runtimeServer);
    if (messagingService instanceof Service) {
      Services.stopAndWait((Service) messagingService);
    }
  }

  @Test
  public void testSmallMessage() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").workflow("workflow").run(RunIds.generate());
    TopicId topicId = NamespaceId.SYSTEM.topic("topic");

    List<Message> messages = new ArrayList<>();
    messages.add(createMessage(Math.max(1, RuntimeClient.CHUNK_SIZE / 4)));

    runtimeClient.sendMessages(programRunId, topicId, messages.iterator());
    assertMessages(topicId, messages);
  }

  @Test
  public void testLargeMessage() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").workflow("workflow").run(RunIds.generate());
    TopicId topicId = NamespaceId.SYSTEM.topic(TEST_TOPIC);

    List<Message> messages = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      messages.add(createMessage(RuntimeClient.CHUNK_SIZE * 2));
    }

    runtimeClient.sendMessages(programRunId, topicId, messages.iterator());
    assertMessages(topicId, messages);
  }

  @Test
  public void testMixedSendMessage() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").workflow("workflow").run(RunIds.generate());
    TopicId topicId = NamespaceId.SYSTEM.topic("topic");

    List<Message> messages = new ArrayList<>();

    // Generate a mix of large and small messages
    for (int i = 0; i < 10; i++) {
      messages.add(createMessage(i + 1));
    }
    for (int i = 0; i < 10; i++) {
      messages.add(createMessage(i + RuntimeClient.CHUNK_SIZE));
    }

    runtimeClient.sendMessages(programRunId, topicId, messages.iterator());
    assertMessages(topicId, messages);
  }

  @Test
  public void testLogMessage() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").workflow("workflow").run(RunIds.generate());
    TopicId topicId = NamespaceId.SYSTEM.topic(cConf.get(Constants.Logging.TMS_TOPIC_PREFIX) + "0");

    List<Message> messages = IntStream.range(0, 100).mapToObj(this::createMessage).collect(Collectors.toList());
    runtimeClient.sendMessages(programRunId, topicId, messages.iterator());

    List<String> expected = messages.stream().map(Message::getPayloadAsString).collect(Collectors.toList());
    Assert.assertEquals(expected, logEntries);
  }

  @Test
  public void testFutureIsNotBlockingWhenValueIsSet() throws Exception {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    runtimeClient.onProgramStopRequested(terminateTs -> countDownLatch.countDown());
    // Now call sendMessages which will set the future
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").workflow("workflow").run(RunIds.generate());
    TopicId topicId = NamespaceId.SYSTEM.topic("topic");
    List<Message> messages = new ArrayList<>();
    messages.add(createMessage(Math.max(1, RuntimeClient.CHUNK_SIZE / 4)));
    runtimeClient.sendMessages(programRunId, topicId, messages.iterator());

    // Assert that future is not blocking anymore
    Assert.assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
  }

  @Test (timeout = 2000L)
  public void testSparkEventLogs() throws Exception {
    File eventFile = TEMP_FOLDER.newFile();
    Files.write(eventFile.toPath(), "Testing".getBytes(StandardCharsets.UTF_8), StandardOpenOption.TRUNCATE_EXISTING);

    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").spark("spark").run(RunIds.generate());
    runtimeClient.uploadSparkEventLogs(programRunId, eventFile);

    // Verify by reading from the event logs location
    List<Location> eventFiles = locationFactory.create(cConf.get(Constants.AppFabric.SPARK_EVENT_LOGS_DIR)).list()
      .stream()
      .filter(l -> l.getName().endsWith(eventFile.getName()))
      .collect(Collectors.toList());

    Assert.assertEquals(1, eventFiles.size());
    try (InputStream is = eventFiles.get(0).getInputStream()) {
      Assert.assertEquals("Testing", new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testRuntimeTokenRefusalHeaderAndClientDetection() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").workflow("workflow")
        .run(RunIds.generate());
    RemoteClient rawClient = remoteClientFactory.createRemoteClient(
        Constants.Service.RUNTIME,
        new DefaultHttpRequestConfig(false),
        Constants.Gateway.INTERNAL_API_VERSION_3 + "/runtime/namespaces/");

    // Report-only / normal 200 has no X-CDAP-Runtime-Token-Failure header.
    assertTopicResponse(rawClient, programRunId, TEST_TOPIC, HttpURLConnection.HTTP_OK, null);

    // Enforcing LEGACY_TOKEN, URL_MISMATCH, and MISSING_SCOPES emit HTTP 403 + failure header,
    // throw RuntimeTokenRefusedException in sendMessages, and wrap it in IOException in
    // uploadSparkEventLogs.
    File eventFile = TEMP_FOLDER.newFile();
    Files.write(eventFile.toPath(), "spark-event".getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.TRUNCATE_EXISTING);
    TopicId topicId = NamespaceId.SYSTEM.topic(TEST_TOPIC);

    for (RuntimeValidationFailure failure : RuntimeValidationFailure.values()) {
      requestValidatorRef.set((runId, req) -> {
        throw new RuntimeTokenRefusedException(failure, "refused: " + failure.getReason());
      });

      assertTopicResponse(rawClient, programRunId, TEST_TOPIC,
          HttpURLConnection.HTTP_FORBIDDEN, failure.getReason());

      RuntimeTokenRefusedException refused = Assert.assertThrows(
          RuntimeTokenRefusedException.class,
          () -> runtimeClient.sendMessages(programRunId, topicId,
              Collections.singletonList(createMessage(16)).iterator()));
      Assert.assertEquals(failure.getReason(), refused.getReason());

      IOException sparkThrown = Assert.assertThrows(
          IOException.class, () -> runtimeClient.uploadSparkEventLogs(programRunId, eventFile));
      Assert.assertTrue("Expected RuntimeTokenRefusedException cause, got "
              + sparkThrown.getCause(),
          sparkThrown.getCause() instanceof RuntimeTokenRefusedException);
      Assert.assertEquals(failure.getReason(),
          ((RuntimeTokenRefusedException) sparkThrown.getCause()).getReason());
    }

    // Wrapped RuntimeTokenRefusedException with null message still emits HTTP 403 + header.
    requestValidatorRef.set((runId, req) -> {
      throw new RuntimeException("wrapped refusal",
          new RuntimeTokenRefusedException(RuntimeValidationFailure.URL_MISMATCH, null));
    });
    assertTopicResponse(rawClient, programRunId, TEST_TOPIC,
        HttpURLConnection.HTTP_FORBIDDEN, RuntimeValidationFailure.URL_MISMATCH.getReason());
    RuntimeTokenRefusedException wrappedThrown = Assert.assertThrows(
        RuntimeTokenRefusedException.class,
        () -> runtimeClient.sendMessages(programRunId, topicId,
            Collections.singletonList(createMessage(16)).iterator()));
    Assert.assertEquals(RuntimeValidationFailure.URL_MISMATCH.getReason(),
        wrappedThrown.getReason());

    // Unallowed topic 403 in RuntimeHandler must NOT emit X-CDAP-Runtime-Token-Failure.
    requestValidatorRef.set((runId, req) -> ValidatedRuntimeRequest.legacy(
        new ProgramRunInfo(Long.MAX_VALUE)));
    assertTopicResponse(rawClient, programRunId, "disallowed-topic",
        HttpURLConnection.HTTP_FORBIDDEN, null);
    Assert.assertThrows(IOException.class,
        () -> runtimeClient.sendMessages(programRunId, NamespaceId.SYSTEM.topic("disallowed-topic"),
            Collections.singletonList(createMessage(16)).iterator()));

    // Generic UnauthorizedException (403) must NOT emit X-CDAP-Runtime-Token-Failure.
    requestValidatorRef.set((runId, req) -> {
      throw new UnauthorizedException("generic forbidden");
    });
    assertTopicResponse(rawClient, programRunId, TEST_TOPIC, HttpURLConnection.HTTP_FORBIDDEN, null);
    Assert.assertThrows(IOException.class,
        () -> runtimeClient.sendMessages(programRunId, topicId,
            Collections.singletonList(createMessage(16)).iterator()));

    // UnauthenticatedException (401) must NOT emit X-CDAP-Runtime-Token-Failure.
    requestValidatorRef.set((runId, req) -> {
      throw new UnauthenticatedException("unauthenticated");
    });
    assertTopicResponse(rawClient, programRunId, TEST_TOPIC,
        HttpURLConnection.HTTP_UNAUTHORIZED, null);
    Assert.assertThrows(IOException.class,
        () -> runtimeClient.sendMessages(programRunId, topicId,
            Collections.singletonList(createMessage(16)).iterator()));
  }

  @Test
  public void testBlankRuntimeTokenFailureHeaderTreatedAsUnmarkedForbidden() throws Exception {
    NettyHttpService stubServer = NettyHttpService.builder("blank-failure-header")
        .setHost(InetAddress.getLocalHost().getCanonicalHostName())
        .setHttpHandlers(new AbstractHttpHandler() {
          @POST
          @Path(Constants.Gateway.INTERNAL_API_VERSION_3
              + "/runtime/namespaces/{ns}/apps/{app}/versions/{ver}/{type}/{prog}/runs/{run}/topics/{topic}")
          public void rejectBlankHeader(HttpRequest request, HttpResponder responder) {
            responder.sendString(HttpResponseStatus.FORBIDDEN, "blank header forbidden",
                new DefaultHttpHeaders().set(
                    Constants.Security.Headers.RUNTIME_TOKEN_FAILURE, "   "));
          }
        })
        .build();
    stubServer.start();
    try {
      InMemoryDiscoveryService stubDiscovery = new InMemoryDiscoveryService();
      stubDiscovery.register(URIScheme.createDiscoverable(Constants.Service.RUNTIME, stubServer));
      RemoteClientFactory stubFactory = new RemoteClientFactory(
          stubDiscovery, new NoOpInternalAuthenticator());
      RuntimeClient stubClient = new RuntimeClient(cConf, stubFactory);
      ProgramRunId runId = NamespaceId.DEFAULT.app("app").workflow("wf").run(RunIds.generate());
      // Blank X-CDAP-Runtime-Token-Failure header must not trigger fail-fast refusal.
      Assert.assertThrows(IOException.class,
          () -> stubClient.sendMessages(runId, NamespaceId.SYSTEM.topic(TEST_TOPIC),
              Collections.singletonList(createMessage(16)).iterator()));
    } finally {
      stubServer.stop();
    }
  }

  @Test
  public void testRuntimeTokenRefusedExceptionValidation() {
    RuntimeTokenRefusedException trimmed =
        new RuntimeTokenRefusedException("  legacy_token  ", "refused");
    Assert.assertEquals("legacy_token", trimmed.getReason());
    Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, trimmed.getStatusCode());

    Assert.assertThrows(IllegalArgumentException.class,
        () -> new RuntimeTokenRefusedException("   ", "refused"));
    Assert.assertThrows(NullPointerException.class,
        () -> new RuntimeTokenRefusedException((String) null, "refused"));
    Assert.assertThrows(NullPointerException.class,
        () -> new RuntimeTokenRefusedException((RuntimeValidationFailure) null, "refused"));
  }

  @Test
  public void testWriteMessagesPublishesAsSystemAndRestoresContext() throws Exception {
    AtomicReference<Callable<Void>> publishHookRef = new AtomicReference<>();
    AtomicReference<RemoteExecutionLogProcessor> logProcessorRef = new AtomicReference<>(
        payloads -> payloads.forEachRemaining(bytes -> { }));
    Injector injector = createSecurityRuntimeInjector(
        publishHookRef, payloads -> logProcessorRef.get().process(payloads));

    RuntimeServer server = injector.getInstance(RuntimeServer.class);
    Services.startAndWait(server);
    try {
      ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").workflow("wf")
          .run(RunIds.generate());
      assertHttpWriteMessagesContext(injector, programRunId, logProcessorRef, publishHookRef);
      assertBodyConsumerRestoresCallerContext(
          injector.getInstance(RuntimeHandler.class), programRunId, publishHookRef);
    } finally {
      Services.stopAndWait(server);
    }
  }

  private Injector createSecurityRuntimeInjector(
      AtomicReference<Callable<Void>> publishHookRef, RemoteExecutionLogProcessor logProcessor) {
    CConfiguration securityConf = CConfiguration.copy(cConf);
    securityConf.setBoolean(Constants.Security.ENABLED, true);
    securityConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    securityConf.setInt(Constants.RuntimeMonitor.BIND_PORT, 0);

    MessagingService interceptingMessaging = createPublishInterceptingMessagingService(
        publishHookRef);
    InMemoryDiscoveryService discovery = new InMemoryDiscoveryService();
    return Guice.createInjector(
        new ConfigModule(securityConf),
        RemoteAuthenticatorModules.getNoOpModule(),
        new NoOpAuditLogModule(),
        new UserCredentialAeadEncryptionModule(),
        new LocalLocationModule(),
        new AuthenticationContextModules().getNoOpModule(),
        new AuthorizationEnforcementModule().getNoOpModules(),
        new RuntimeServerModule() {
          @Override
          protected void configure() {
            super.configure();
            bind(RuntimeHandler.class).in(Scopes.SINGLETON);
            expose(RuntimeHandler.class);
          }

          @Override
          protected void bindRequestValidator() {
            bind(RuntimeRequestValidator.class).toInstance(
                (programRunId, request) -> ValidatedRuntimeRequest.legacy(
                    new ProgramRunInfo(Long.MAX_VALUE)));
          }

          @Override
          protected void bindLogProcessor() {
            bind(RemoteExecutionLogProcessor.class).toInstance(logProcessor);
          }
        },
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(DiscoveryService.class).toInstance(discovery);
            bind(DiscoveryServiceClient.class).toInstance(discovery);
            bind(MessagingService.class).toInstance(interceptingMessaging);
            bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
          }
        });
  }

  private MessagingService createPublishInterceptingMessagingService(
      AtomicReference<Callable<Void>> publishHookRef) {
    return (MessagingService) Proxy.newProxyInstance(
        MessagingService.class.getClassLoader(),
        new Class<?>[]{MessagingService.class},
        (proxy, method, args) -> {
          if (method.getDeclaringClass() != Object.class && "publish".equals(method.getName())) {
            Callable<Void> hook = publishHookRef.get();
            if (hook != null) {
              hook.call();
            }
          }
          try {
            return method.invoke(messagingService, args);
          } catch (InvocationTargetException e) {
            throw e.getTargetException();
          }
        });
  }

  private void assertHttpWriteMessagesContext(Injector injector, ProgramRunId programRunId,
      AtomicReference<RemoteExecutionLogProcessor> logProcessorRef,
      AtomicReference<Callable<Void>> publishHookRef) throws Exception {
    String tokenValue = "runtime-token-value";
    RemoteClientFactory clientFactory = new RemoteClientFactory(
        injector.getInstance(DiscoveryServiceClient.class),
        headerSetter -> headerSetter.accept(Constants.Security.Headers.RUNTIME_TOKEN,
            Credential.CREDENTIAL_TYPE_INTERNAL + " " + tokenValue));
    RuntimeClient client = new RuntimeClient(
        injector.getInstance(CConfiguration.class), clientFactory);

    assertHttpLogTopicRetainsCallerContext(client, programRunId, tokenValue, logProcessorRef);
    assertHttpNonLogTopicPublishesAsSystem(client, programRunId, publishHookRef);
  }

  private void assertHttpLogTopicRetainsCallerContext(RuntimeClient client,
      ProgramRunId programRunId, String expectedTokenValue,
      AtomicReference<RemoteExecutionLogProcessor> logProcessorRef) throws Exception {
    AtomicReference<String> logUserId = new AtomicReference<>();
    AtomicReference<String> logUserIp = new AtomicReference<>();
    AtomicReference<Credential> logCredential = new AtomicReference<>();
    AtomicInteger logCalls = new AtomicInteger();
    logProcessorRef.set(payloads -> {
      logUserId.set(SecurityRequestContext.getUserId());
      logUserIp.set(SecurityRequestContext.getUserIp());
      logCredential.set(SecurityRequestContext.getUserCredential());
      logCalls.incrementAndGet();
      payloads.forEachRemaining(bytes -> { });
    });

    TopicId logTopic = NamespaceId.SYSTEM.topic(
        cConf.get(Constants.Logging.TMS_TOPIC_PREFIX) + "0");
    client.sendMessages(programRunId, logTopic,
        Collections.singletonList(createMessage(16)).iterator());
    Assert.assertTrue(logCalls.get() >= 1);
    Assert.assertEquals(Constants.Security.Authentication.RUNTIME_IDENTITY, logUserId.get());
    Assert.assertNotNull(logUserIp.get());
    Assert.assertNotNull(logCredential.get());
    Assert.assertEquals(Credential.CredentialType.INTERNAL, logCredential.get().getType());
    Assert.assertEquals(expectedTokenValue, logCredential.get().getValue());
  }

  private void assertHttpNonLogTopicPublishesAsSystem(RuntimeClient client,
      ProgramRunId programRunId, AtomicReference<Callable<Void>> publishHookRef) throws Exception {
    AtomicInteger publishCalls = new AtomicInteger();
    AtomicReference<String> publishUserId = new AtomicReference<>("unset");
    AtomicReference<String> publishUserIp = new AtomicReference<>("unset");
    AtomicReference<Credential> publishCredential = new AtomicReference<>(
        new Credential("unset", Credential.CredentialType.INTERNAL));
    publishHookRef.set(() -> {
      publishUserId.set(SecurityRequestContext.getUserId());
      publishUserIp.set(SecurityRequestContext.getUserIp());
      publishCredential.set(SecurityRequestContext.getUserCredential());
      publishCalls.incrementAndGet();
      return null;
    });
    try {
      TopicId topicId = NamespaceId.SYSTEM.topic(TEST_TOPIC);
      List<Message> messages = Collections.singletonList(createMessage(16));
      client.sendMessages(programRunId, topicId, messages.iterator());
      Assert.assertTrue(publishCalls.get() >= 1);
      Assert.assertNull(publishUserId.get());
      Assert.assertNull(publishUserIp.get());
      Assert.assertNull(publishCredential.get());
      assertMessages(topicId, messages);
    } finally {
      publishHookRef.set(null);
    }
  }

  private void assertBodyConsumerRestoresCallerContext(RuntimeHandler handler,
      ProgramRunId programRunId, AtomicReference<Callable<Void>> publishHookRef) throws Exception {
    String callerUserId = "caller-user";
    String callerUserIp = "10.1.2.3";
    Credential callerCredential = new Credential(
        "caller-runtime-token", Credential.CredentialType.INTERNAL);

    SecurityRequestContext.setUserId(callerUserId);
    SecurityRequestContext.setUserIp(callerUserIp);
    SecurityRequestContext.setUserCredential(callerCredential);
    try {
      Thread callerThread = Thread.currentThread();
      AtomicInteger syncPublishCalls = new AtomicInteger();
      publishHookRef.set(() -> {
        Assert.assertSame(callerThread, Thread.currentThread());
        assertClearedSecurityContext();
        syncPublishCalls.incrementAndGet();
        return null;
      });

      MockResponder okResponder = new MockResponder();
      BodyConsumer okConsumer = openWriteMessagesConsumer(handler, programRunId, okResponder);
      // Avro binary array of 1 bytes element: count=1 (zigzag 2), len=1 (zigzag 2), 'a', end=0.
      byte[] avroSingleMessage = new byte[]{2, 2, 'a', 0};
      okConsumer.chunk(Unpooled.wrappedBuffer(avroSingleMessage), okResponder);
      okConsumer.finished(okResponder);

      Assert.assertEquals(1, syncPublishCalls.get());
      Assert.assertEquals(HttpResponseStatus.OK, okResponder.getStatus());
      assertSecurityContext(callerUserId, callerUserIp, callerCredential);

      // Error path: TopicNotFoundException -> HTTP 400 (BadRequestException) and context restored.
      publishHookRef.set(() -> {
        assertClearedSecurityContext();
        throw new TopicNotFoundException(NamespaceId.SYSTEM.getNamespace(), TEST_TOPIC);
      });
      MockResponder errorResponder = new MockResponder();
      BodyConsumer errorConsumer = openWriteMessagesConsumer(handler, programRunId, errorResponder);
      ByteBuf errorChunk = Unpooled.wrappedBuffer(avroSingleMessage);
      RuntimeException thrown = Assert.assertThrows(
          RuntimeException.class, () -> errorConsumer.chunk(errorChunk, errorResponder));
      Assert.assertTrue("Expected BadRequestException cause, got " + thrown.getCause(),
          thrown.getCause() instanceof BadRequestException);
      Throwable rootCause = thrown.getCause().getCause();
      Assert.assertTrue("Expected TopicNotFoundException cause, got " + rootCause,
          rootCause instanceof TopicNotFoundException);
      Assert.assertEquals(HttpResponseStatus.BAD_REQUEST, errorResponder.getStatus());
      assertSecurityContext(callerUserId, callerUserIp, callerCredential);
    } finally {
      publishHookRef.set(null);
      SecurityRequestContext.reset();
    }
  }

  private BodyConsumer openWriteMessagesConsumer(RuntimeHandler handler, ProgramRunId programRunId,
      HttpResponder responder) throws Exception {
    DefaultHttpRequest httpRequest = new DefaultHttpRequest(
        HttpVersion.HTTP_1_1, io.netty.handler.codec.http.HttpMethod.POST, "/topics/" + TEST_TOPIC);
    httpRequest.headers().set(HttpHeaderNames.CONTENT_TYPE, "avro/binary");
    return handler.writeMessages(httpRequest, responder,
        programRunId.getNamespace(), programRunId.getApplication(), programRunId.getVersion(),
        programRunId.getType().getCategoryName(), programRunId.getProgram(),
        programRunId.getRun(), TEST_TOPIC);
  }

  private static void assertClearedSecurityContext() {
    Assert.assertNull(SecurityRequestContext.getUserId());
    Assert.assertNull(SecurityRequestContext.getUserIp());
    Assert.assertNull(SecurityRequestContext.getUserCredential());
  }

  private static void assertSecurityContext(String expectedUserId, String expectedUserIp,
      Credential expectedCredential) {
    Assert.assertEquals(expectedUserId, SecurityRequestContext.getUserId());
    Assert.assertEquals(expectedUserIp, SecurityRequestContext.getUserIp());
    Assert.assertSame(expectedCredential, SecurityRequestContext.getUserCredential());
  }

  private void assertTopicResponse(RemoteClient rawClient, ProgramRunId programRunId,
      String topic, int expectedStatus, @Nullable String expectedFailureReason) throws IOException {
    HttpURLConnection conn = openTopicConnection(rawClient, programRunId, topic);
    try {
      Assert.assertEquals(expectedStatus, conn.getResponseCode());
      Assert.assertEquals(expectedFailureReason,
          conn.getHeaderField(Constants.Security.Headers.RUNTIME_TOKEN_FAILURE));
    } finally {
      conn.disconnect();
    }
  }

  private HttpURLConnection openTopicConnection(RemoteClient rawClient, ProgramRunId programRunId,
      String topic) throws IOException {
    String path = String.format("%s/apps/%s/versions/%s/%s/%s/runs/%s/topics/%s",
        programRunId.getNamespace(), programRunId.getApplication(), programRunId.getVersion(),
        programRunId.getType().getCategoryName(), programRunId.getProgram(), programRunId.getRun(),
        topic);
    HttpURLConnection conn = rawClient.openConnection(HttpMethod.POST, path);
    conn.setRequestProperty("Content-Type", "avro/binary");
    conn.setDoOutput(true);
    try (OutputStream out = conn.getOutputStream()) {
      out.write(0);
    }
    return conn;
  }

  private void assertMessages(TopicId topicId, Collection<Message> messages) throws Exception {
    // Read the messages from TMS and compare
    MessagingContext messagingContext = new MultiThreadMessagingContext(messagingService);
    List<Message> fetchedMessages = new ArrayList<>();
    try (CloseableIterator<Message> iterator =
           messagingContext.getMessageFetcher()
             .fetch(topicId.getNamespace(), topicId.getTopic(), Integer.MAX_VALUE, null)) {
      iterator.forEachRemaining(fetchedMessages::add);
    }

    Assert.assertEquals(
      messages.stream().map(Message::getPayloadAsString).collect(Collectors.toList()),
      fetchedMessages.stream().map(Message::getPayloadAsString).collect(Collectors.toList())
    );
  }

  /**
   * Creates a {@link Message} with a payload of the given size.
   */
  private Message createMessage(int size) {
    String messageId = RunIds.generate().getId();
    byte[] payload = Strings.repeat("m", size).getBytes(StandardCharsets.UTF_8);

    return new Message() {
      @Override
      public String getId() {
        return messageId;
      }

      @Override
      public byte[] getPayload() {
        return payload;
      }
    };
  }
}
