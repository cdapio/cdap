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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.retry.Idempotency;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.app.guice.RuntimeServerModule;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ServiceException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.encryption.guice.UserCredentialAeadEncryptionModule;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.guice.NoOpAuditLogModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.BinaryEncoder;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.io.DatumWriter;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.service.ServiceDiscoverable;
import io.cdap.cdap.common.service.Services;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data.runtime.ConstantTransactionSystemClient;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import io.cdap.cdap.data2.metadata.writer.MetadataMessage;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.monitor.RemoteExecutionLogProcessor;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeClient;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeClientService;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeProgramStatusSubscriberService;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeScopes;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeServer;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeTokenRefusedException;
import io.cdap.cdap.internal.app.runtime.plugin.ConnectionPropertiesFetcher;
import io.cdap.cdap.internal.app.runtime.plugin.RuntimeScopesExtractor;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.io.ASMDatumWriterFactory;
import io.cdap.cdap.internal.io.ASMFieldAccessorFactory;
import io.cdap.cdap.internal.io.ReflectionSchemaGenerator;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.logging.appender.LogMessage;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordFetcher;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.DatasetInstanceConfiguration;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.audit.AuditMessage;
import io.cdap.cdap.proto.audit.AuditType;
import io.cdap.cdap.proto.audit.payload.access.AccessPayload;
import io.cdap.cdap.proto.audit.payload.access.AccessType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.cdap.security.spi.authorization.AccessControllerSpi;
import io.cdap.cdap.security.spi.authorization.NoOpAccessController;
import io.cdap.cdap.security.spi.authorization.PermissionManager;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.spi.data.transaction.TxCallable;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * End-to-end tests wiring {@link RuntimeServer}, {@link RuntimeProgramStatusSubscriberService},
 * {@link ConnectionPropertiesFetcher}, {@link RuntimeScopesExtractor},
 * {@link RemoteExecutionTwillRunnerService}, {@link RemoteClient}, and {@link RuntimeClient} with
 * security and internal authentication enabled.
 */
public class RuntimeTokenEndToEndTest {

  private static final Gson GSON = new Gson();
  private static final NamespaceId NS1 = new NamespaceId("ns1");
  private static final NamespaceId NS2 = new NamespaceId("ns2");
  private static final ArtifactId PIPELINE_ARTIFACT = new ArtifactId(
      "cdap-data-pipeline", new ArtifactVersion("6.12.0"), ArtifactScope.SYSTEM);
  private static final String STUDIO_SERVICE = ServiceDiscoverable.getName(
      NamespaceId.SYSTEM.getNamespace(), "pipeline", ProgramType.SERVICE, "studio");
  private static final long POLL_TIME_MS = 100L;
  private static final long REPLICATION_TIMEOUT_MS = 10_000L;
  private static final long EXCHANGE_TOKEN_TTL_MS = TimeUnit.MINUTES.toMillis(5);
  private static final String CONTAINER_USER = "container-user";

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

  private CConfiguration baseConf;
  private InMemoryDiscoveryService discoveryService;
  private MessagingService messagingService;
  private MessagingContext messagingContext;
  private TransactionRunner txRunner;
  private TokenManager tokenManager;
  private AccessTokenCodec accessTokenCodec;
  private LocationFactory locationFactory;
  private RemoteExecutionTwillRunnerService runnerService;
  private RuntimeProgramStatusSubscriberService statusSubscriber;
  private MetricsCollectionService countingMetricsService;
  private Map<String, Long> counters;
  private Map<TopicId, String> topicCursors;
  private List<byte[]> receivedLogs;
  private List<NettyHttpService> mockServices;
  private List<Cancellable> discoveryRegistrations;
  private MockAppFabricHandler appFabricHandler;
  private MockStudioHandler studioHandler;
  private MockDatasetHandler datasetHandler;
  private RuntimeServer runtimeServer;
  private AtomicInteger keyDirCounter;

  @Before
  public void setUp() throws Exception {
    keyDirCounter = new AtomicInteger();
    counters = new ConcurrentHashMap<>();
    topicCursors = new ConcurrentHashMap<>();
    receivedLogs = new CopyOnWriteArrayList<>();
    mockServices = new ArrayList<>();
    discoveryRegistrations = new ArrayList<>();
    discoveryService = new InMemoryDiscoveryService();
    countingMetricsService = createCountingMetricsService(counters);

    baseConf = CConfiguration.create();
    baseConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder("data").getAbsolutePath());
    baseConf.setBoolean(Constants.Security.ENABLED, true);
    baseConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    baseConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    baseConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    baseConf.setBoolean(Constants.AppFabric.SPARK_EVENT_LOGS_ENABLED, true);
    baseConf.set(Constants.RuntimeMonitor.MONITOR_URL, "https://monitor.example.com");
    baseConf.set(Constants.RuntimeMonitor.MONITOR_TYPE_PREFIX + "gcp-dataproc",
        RuntimeMonitorType.URL.name());
    baseConf.set(Constants.RuntimeMonitor.BIND_ADDRESS, "127.0.0.1");
    baseConf.setInt(Constants.RuntimeMonitor.BIND_PORT, 0);
    baseConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, POLL_TIME_MS);
    baseConf.setLong(Constants.AppFabric.STATUS_EVENT_POLL_DELAY_MILLIS, 10L);
    baseConf.set(Constants.RuntimeMonitor.RUN_RECORD_FETCHER_CLASS,
        RemoteProgramRunRecordFetcher.class.getName());
    baseConf.set(Constants.RuntimeMonitor.TOPICS_CONFIGS,
        Constants.Audit.TOPIC + ","
            + Constants.Dataset.DATA_EVENT_TOPIC + ","
            + Constants.Metadata.MESSAGING_TOPIC + ","
            + Constants.Metrics.TOPIC_PREFIX + ":1,"
            + Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC + ","
            + Constants.Logging.TMS_TOPIC_PREFIX + ":1");

    Injector infraInjector = Guice.createInjector(
        new ConfigModule(baseConf, new Configuration(), SConfiguration.create()),
        new IOModule(),
        new NoOpAuditLogModule(),
        new UserCredentialAeadEncryptionModule(),
        new LocalLocationModule(),
        new SystemDatasetRuntimeModule().getInMemoryModules(),
        new DataSetsModules().getInMemoryModules(),
        new NamespaceAdminTestModule(),
        new StorageModule(),
        new MessagingServerRuntimeModule().getInMemoryModules(),
        new CoreSecurityRuntimeModule().getInMemoryModules(),
        new AuthenticationContextModules().getNoOpModule(),
        new AuthorizationEnforcementModule().getNoOpModules(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).toInstance(countingMetricsService);
            bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class);
            bind(Store.class).to(DefaultStore.class);
            bind(DiscoveryService.class).toInstance(discoveryService);
            bind(DiscoveryServiceClient.class).toInstance(discoveryService);
          }
        });

    StoreDefinition.AppMetadataStore.create(
        infraInjector.getInstance(StructuredTableAdmin.class));

    txRunner = infraInjector.getInstance(TransactionRunner.class);
    locationFactory = infraInjector.getInstance(LocationFactory.class);
    TokenManager tm = infraInjector.getInstance(TokenManager.class);
    Services.startAndWait(tm);
    tokenManager = tm;
    accessTokenCodec = infraInjector.getInstance(AccessTokenCodec.class);

    MessagingService ms = infraInjector.getInstance(MessagingService.class);
    if (ms instanceof Service) {
      Services.startAndWait((Service) ms);
    }
    messagingService = ms;
    createTopics(
        baseConf.get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC),
        baseConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC),
        baseConf.get(Constants.Logging.TMS_TOPIC_PREFIX) + "0",
        baseConf.get(Constants.Metrics.TOPIC_PREFIX) + "0",
        baseConf.get(Constants.Metadata.MESSAGING_TOPIC),
        baseConf.get(Constants.Audit.TOPIC),
        baseConf.get(Constants.Dataset.DATA_EVENT_TOPIC));
    messagingContext = new MultiThreadMessagingContext(messagingService);

    runnerService = new RemoteExecutionTwillRunnerService(
        baseConf,
        new Configuration(),
        discoveryService,
        locationFactory,
        Mockito.mock(ProvisioningService.class),
        Mockito.mock(ProgramStateWriter.class),
        txRunner,
        accessTokenCodec,
        tokenManager);

    RuntimeProgramStatusSubscriberService subscriber =
        infraInjector.getInstance(RuntimeProgramStatusSubscriberService.class);
    Services.startAndWait(subscriber);
    statusSubscriber = subscriber;

    appFabricHandler = new MockAppFabricHandler();
    startMockService("mock-appfabric", appFabricHandler,
        Constants.Service.APP_FABRIC_HTTP, Constants.Service.SECURE_STORE_SERVICE);

    studioHandler = new MockStudioHandler();
    startMockService("mock-studio", studioHandler, STUDIO_SERVICE);

    datasetHandler = new MockDatasetHandler();
    startMockService("mock-dataset", datasetHandler, Constants.Service.DATASET_MANAGER);
  }

  @After
  public void tearDown() {
    try {
      stopQuietly(() -> {
        if (runtimeServer != null) {
          Services.stopAndWait(runtimeServer);
          runtimeServer = null;
        }
      });
      stopQuietly(() -> {
        if (statusSubscriber != null) {
          Services.stopAndWait(statusSubscriber);
        }
      });
      if (discoveryRegistrations != null) {
        for (Cancellable registration : discoveryRegistrations) {
          stopQuietly(registration::cancel);
        }
      }
      if (mockServices != null) {
        for (NettyHttpService service : mockServices) {
          stopQuietly(service::stop);
        }
      }
      stopQuietly(() -> {
        if (messagingService instanceof Service) {
          Services.stopAndWait((Service) messagingService);
        }
      });
      stopQuietly(() -> {
        if (tokenManager != null) {
          Services.stopAndWait(tokenManager);
        }
      });
    } finally {
      InMemoryTableService.reset();
      SecurityRequestContext.reset();
    }
  }

  @Test
  public void testEndToEndReportOnlyToEnforcingRestartWhileRunActive() throws Exception {
    ProgramRunId wfRun = NS1.app("pipe1", "1.0").workflow("DataPipelineWorkflow")
        .run(RunIds.generate());
    // Compute scopes via real ConnectionPropertiesFetcher -> MockStudioHandler -> RuntimeScopesExtractor
    ConnectionPropertiesFetcher fetcher = new ConnectionPropertiesFetcher(
        new RemoteClientFactory(discoveryService, setter -> { }));
    RuntimeScopes scopes = RuntimeScopesExtractor.extract(
        appSpecWithConnectionMacro(wfRun.getParent(), "${conn(conn_ok)}"),
        wfRun.getParent(),
        Collections.emptyMap(),
        null,
        fetcher.forNamespace(NS1.getNamespace()));
    Assert.assertEquals(Collections.singleton("conn_ok"), scopes.getConnections());
    Assert.assertEquals(Collections.singleton("prov/cred-ok"), scopes.getOauth());
    Assert.assertEquals(Collections.singleton("sec-ok"), scopes.getSecrets());
    Assert.assertFalse(scopes.hasExternalDynamicSecret());

    // Boot RuntimeServer in report-only mode and replicate wfRun via TMS
    runtimeServer = startRuntimeServer(false);
    publishRecordedStarting(wfRun, ImmutableMap.of(
        SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc",
        ProgramOptionConstants.RUNTIME_SCOPES, scopes.toJson()));
    publishRecordedRunning(wfRun);
    awaitLocalRun(wfRun, ProgramRunStatus.RUNNING);

    String runtimeToken = mintToken(wfRun, scopedOptions(wfRun, scopes));
    String legacyToken = mintToken(wfRun, unscopedOptions(wfRun));
    RemoteClient runtimeCallbackClient = createCallbackClient(runtimeToken);
    RemoteClient legacyCallbackClient = createCallbackClient(legacyToken);
    RuntimeClient runtimeMsgClient = createRuntimeClient(runtimeToken);
    ProgramRunId otherRun = NS1.app("otherApp", "1.0").workflow("OtherWf").run(RunIds.generate());
    TopicId logTopic = NamespaceId.SYSTEM.topic(baseConf.get(Constants.Logging.TMS_TOPIC_PREFIX) + "0");
    TopicId metricsTopic = NamespaceId.SYSTEM.topic(baseConf.get(Constants.Metrics.TOPIC_PREFIX) + "0");
    TopicId statusTopic = NamespaceId.SYSTEM.topic(
        baseConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC));

    // Phase 1: Report-only mode (validation = false)
    verifyReportOnlyPhase(runtimeCallbackClient, legacyCallbackClient, runtimeMsgClient,
        runtimeToken, legacyToken, wfRun, otherRun, logTopic, metricsTopic, statusTopic);

    // Restart RuntimeServer with validation = true while wfRun stays active
    Services.stopAndWait(runtimeServer);
    runtimeServer = startRuntimeServer(true);

    // Phase 2: Enforcing mode (validation = true) on the same active run and token
    verifyEnforcingCallbacksAndAuth(runtimeCallbackClient, legacyCallbackClient,
        runtimeToken, legacyToken, wfRun);
    verifyInnerChildRunsAndUrlMismatch(runtimeCallbackClient, runtimeMsgClient,
        wfRun, otherRun, logTopic, statusTopic);
    verifyEnforcingMessages(runtimeMsgClient, wfRun, otherRun, logTopic, metricsTopic, statusTopic);
    verifyTamperedRuntimeToken(runtimeToken, wfRun, logTopic);
    verifyColdStoreRefillAndCompletion(runtimeCallbackClient, runtimeToken, wfRun, scopes);
  }

  @Test
  public void testRestartFromEnforcingToReportOnlyWhileRunActive() throws Exception {
    RuntimeScopes scopes = new RuntimeScopes(
        Collections.emptyList(), Collections.emptyList(),
        Collections.singletonList("sec-ok"), false);
    ProgramRunId wfRun = NS1.app("pipe2", "1.0").workflow("DataPipelineWorkflow")
        .run(RunIds.generate());

    // Start in enforcing mode
    runtimeServer = startRuntimeServer(true);
    publishRecordedStarting(wfRun, ImmutableMap.of(
        SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc",
        ProgramOptionConstants.RUNTIME_SCOPES, scopes.toJson()));
    publishRecordedRunning(wfRun);
    awaitLocalRun(wfRun, ProgramRunStatus.RUNNING);

    String runtimeToken = mintToken(wfRun, scopedOptions(wfRun, scopes));
    RemoteClient client = createCallbackClient(runtimeToken);
    int callsBefore = appFabricHandler.capture.getCallCount();
    assertCallbackForbidden(client, wfRun, Constants.Service.SECURE_STORE_SERVICE,
        "v3/namespaces/ns1/securekeys/sec-bad");
    Assert.assertEquals(callsBefore, appFabricHandler.capture.getCallCount());
    Assert.assertEquals(1L, counter("runtime.token.validation.failure.secret"));

    // Restart RuntimeServer with validation = false while wfRun stays active
    Services.stopAndWait(runtimeServer);
    runtimeServer = startRuntimeServer(false);

    // Same client and same token now pass through in report-only mode with exchanged token
    assertCallbackStatus(client, wfRun, Constants.Service.SECURE_STORE_SERVICE,
        "v3/namespaces/ns1/securekeys/sec-bad", HttpURLConnection.HTTP_OK);
    assertExchangedInternalToken(appFabricHandler.capture.pollLast(), runtimeToken);
    Assert.assertEquals(callsBefore + 1, appFabricHandler.capture.getCallCount());
    Assert.assertEquals(2L, counter("runtime.token.validation.failure.secret"));
  }

  @Test
  public void testUserArgSshMonitorTypeStillIssuesRuntimeTokenAndEnforces() throws Exception {
    ProgramRunId wfRun = NS1.app("pipeSshArg", "1.0").workflow("DataPipelineWorkflow")
        .run(RunIds.generate());
    Map<String, String> userArgs = Collections.singletonMap(
        SystemArguments.RUNTIME_MONITOR_TYPE, "ssh");
    Map<String, String> launchSysArgs = Collections.singletonMap(
        SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc");
    Map<String, String> resolvedUserArgs = SystemArguments.resolveScopedUserArguments(
        wfRun.getParent(), launchSysArgs, userArgs);

    // Operator-configured URL monitor type on gcp-dataproc cannot be overridden by user arg
    Assert.assertEquals(RuntimeMonitorType.URL,
        SystemArguments.getRuntimeMonitorType(baseConf, launchSysArgs, resolvedUserArgs));

    ConnectionPropertiesFetcher fetcher = new ConnectionPropertiesFetcher(
        new RemoteClientFactory(discoveryService, setter -> { }));
    RuntimeScopes scopes = RuntimeScopesExtractor.extract(
        appSpecWithConnectionMacro(wfRun.getParent(), "${conn(conn_ok)}"),
        wfRun.getParent(), resolvedUserArgs, null, fetcher.forNamespace(NS1.getNamespace()));
    Assert.assertEquals(Collections.singleton("sec-ok"), scopes.getSecrets());

    runtimeServer = startRuntimeServer(true);
    publishRecordedStarting(wfRun, ImmutableMap.of(
        SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc",
        ProgramOptionConstants.RUNTIME_SCOPES, scopes.toJson()));
    publishRecordedRunning(wfRun);
    awaitLocalRun(wfRun, ProgramRunStatus.RUNNING);

    ProgramOptions optionsWithSshUserArg = new SimpleProgramOptions(
        wfRun.getParent(),
        new BasicArguments(ImmutableMap.of(
            SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc",
            ProgramOptionConstants.RUNTIME_SCOPES, scopes.toJson())),
        new BasicArguments(resolvedUserArgs));
    Assert.assertTrue(SystemArguments.usesRuntimeToken(baseConf, optionsWithSshUserArg));

    String mintedToken = mintToken(wfRun, optionsWithSshUserArg);
    AccessToken decoded = accessTokenCodec.decode(Base64.getDecoder().decode(mintedToken));
    Assert.assertEquals(UserIdentity.IdentifierType.RUNTIME,
        decoded.getIdentifier().getIdentifierType());
    Assert.assertEquals(wfRun,
        GSON.fromJson(decoded.getIdentifier().getUsername(), ProgramRunId.class));

    RemoteClient callbackClient = createCallbackClient(mintedToken);

    // Allowed callback passes with exchanged INTERNAL token; out-of-scope & disallowed API fail
    assertCallbackStatus(callbackClient, wfRun, Constants.Service.SECURE_STORE_SERVICE,
        "v3/namespaces/ns1/securekeys/sec-ok", HttpURLConnection.HTTP_OK);
    assertExchangedInternalToken(appFabricHandler.capture.pollLast(), mintedToken);
    assertCallbackForbidden(callbackClient, wfRun, Constants.Service.SECURE_STORE_SERVICE,
        "v3/namespaces/ns1/securekeys/sec-bad");
    Assert.assertEquals(1L, counter("runtime.token.validation.failure.secret"));
    assertCallbackForbidden(callbackClient, wfRun, Constants.Service.APP_FABRIC_HTTP,
        "v3/namespaces/ns1/apps");
    Assert.assertEquals(1L, counter("runtime.token.validation.failure.api_not_allowed"));

    // Message validation still drops foreign-namespace data events while relaying own-namespace ones
    RuntimeClient msgClient = createRuntimeClient(mintedToken);
    TopicId dataEventTopic = NamespaceId.SYSTEM.topic(
        baseConf.get(Constants.Dataset.DATA_EVENT_TOPIC));
    Message ownEvent = encodeDataEventMessage(NS1.dataset("ds1"));
    Message foreignEvent = encodeDataEventMessage(NS2.dataset("ds1"));
    assertSingleRelayedMessage(msgClient, wfRun, dataEventTopic, ownEvent, foreignEvent,
        "runtime.token.validation.failure.message_data_event", 1L);

    // Legacy INTERNAL token is rejected for this run in enforcing mode
    String legacyToken = mintToken(wfRun, unscopedOptions(wfRun));
    assertCallbackForbidden(createCallbackClient(legacyToken), wfRun,
        Constants.Service.SECURE_STORE_SERVICE, "v3/namespaces/ns1/securekeys/sec-ok",
        "legacy_token");
    Assert.assertEquals(1L, counter("runtime.token.validation.failure.legacy_token"));
  }

  private void verifyReportOnlyPhase(RemoteClient runtimeCallbackClient,
      RemoteClient legacyCallbackClient, RuntimeClient runtimeMsgClient, String runtimeToken,
      String legacyToken, ProgramRunId wfRun, ProgramRunId otherRun, TopicId logTopic,
      TopicId metricsTopic, TopicId statusTopic) throws Exception {
    // Allowed callbacks succeed, exchange RUNTIME -> INTERNAL token, set CDAP-UserId=runtime,
    // and strip outbound Authorization header
    assertCallbackStatus(runtimeCallbackClient, wfRun, Constants.Service.SECURE_STORE_SERVICE,
        "v3/namespaces/ns1/securekeys/sec-ok", HttpURLConnection.HTTP_OK);
    assertExchangedInternalToken(appFabricHandler.capture.pollLast(), runtimeToken);

    assertCallbackStatus(runtimeCallbackClient, wfRun, STUDIO_SERVICE,
        "v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/ns1/connections/conn_ok",
        HttpURLConnection.HTTP_OK);
    assertExchangedInternalToken(studioHandler.capture.pollLast(), runtimeToken);

    assertCallbackStatus(runtimeCallbackClient, wfRun, STUDIO_SERVICE,
        "v3/namespaces/system/apps/pipeline/services/studio/methods/v1/oauth/provider/prov/credential/cred-ok",
        HttpURLConnection.HTTP_OK);
    assertExchangedInternalToken(studioHandler.capture.pollLast(), runtimeToken);

    // Out-of-scope secret and disallowed API are forwarded in report-only and counted
    assertCallbackStatus(runtimeCallbackClient, wfRun, Constants.Service.SECURE_STORE_SERVICE,
        "v3/namespaces/ns1/securekeys/sec-bad", HttpURLConnection.HTTP_OK);
    Assert.assertEquals(1L, counter("runtime.token.validation.failure.secret"));

    assertCallbackStatus(runtimeCallbackClient, wfRun, Constants.Service.APP_FABRIC_HTTP,
        "v3/namespaces/ns1/apps", HttpURLConnection.HTTP_OK);
    Assert.assertEquals(1L, counter("runtime.token.validation.failure.api_not_allowed"));

    // Legacy INTERNAL token on scoped run is forwarded un-exchanged in report-only and counted,
    // with RuntimeIdentityHandler's CDAP-UserId=runtime and stripped Authorization header
    assertCallbackStatus(legacyCallbackClient, wfRun, Constants.Service.SECURE_STORE_SERVICE,
        "v3/namespaces/ns1/securekeys/sec-ok", HttpURLConnection.HTTP_OK);
    CapturedRequest legacyCaptured = appFabricHandler.capture.pollLast();
    Assert.assertNotNull(legacyCaptured);
    Assert.assertEquals(Credential.CREDENTIAL_TYPE_INTERNAL + " " + legacyToken,
        legacyCaptured.runtimeTokenHeader);
    Assert.assertEquals(Constants.Security.Authentication.RUNTIME_IDENTITY,
        legacyCaptured.userIdHeader);
    Assert.assertNull(legacyCaptured.authorizationHeader);
    Assert.assertEquals(1L, counter("runtime.token.validation.failure.legacy_token"));

    // RuntimeClient messages: own + foreign metric and status are both relayed in report-only
    runtimeMsgClient.sendMessages(wfRun, logTopic,
        Collections.singletonList(encodeLogMessage(wfRun, "own-log-report-only")).iterator());
    Assert.assertEquals(1, receivedLogs.size());

    runtimeMsgClient.sendMessages(wfRun, metricsTopic, Arrays.asList(
        encodeMetricMessage(wfRun, "m.own", 1L),
        encodeMetricMessage(otherRun, "m.other", 2L)).iterator());
    Assert.assertEquals(2, fetchNewPayloads(metricsTopic).size());
    Assert.assertEquals(1L, counter("runtime.token.validation.failure.message_metrics"));

    runtimeMsgClient.sendMessages(wfRun, statusTopic, Arrays.asList(
        encodeHeartbeatMessage(wfRun),
        encodeStatusMessage(otherRun, ProgramRunStatus.FAILED, Collections.emptyMap())).iterator());
    Assert.assertEquals(2, fetchNewPayloads(statusTopic).size());
    Assert.assertEquals(1L, counter("runtime.token.validation.failure.message_status"));
  }

  private void verifyEnforcingCallbacksAndAuth(RemoteClient runtimeCallbackClient,
      RemoteClient legacyCallbackClient, String runtimeToken, String legacyToken,
      ProgramRunId wfRun) throws Exception {
    // Allowed GET callbacks continue to succeed with exchanged INTERNAL tokens
    assertCallbackStatus(runtimeCallbackClient, wfRun, Constants.Service.SECURE_STORE_SERVICE,
        "v3/namespaces/ns1/securekeys/sec-ok", HttpURLConnection.HTTP_OK);
    assertExchangedInternalToken(appFabricHandler.capture.pollLast(), runtimeToken);

    assertCallbackStatus(runtimeCallbackClient, wfRun, STUDIO_SERVICE,
        "v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/ns1/connections/conn_ok",
        HttpURLConnection.HTTP_OK);
    assertExchangedInternalToken(studioHandler.capture.pollLast(), runtimeToken);

    assertCallbackStatus(runtimeCallbackClient, wfRun, STUDIO_SERVICE,
        "v3/namespaces/system/apps/pipeline/services/studio/methods/v1/oauth/provider/prov/credential/cred-ok",
        HttpURLConnection.HTTP_OK);
    assertExchangedInternalToken(studioHandler.capture.pollLast(), runtimeToken);

    // Allowed and disallowed PUT/DELETE body-rule callbacks on dataset.service
    String localDatasetName = "localDs." + wfRun.getRun();
    String tableDatasetBody = GSON.toJson(new DatasetInstanceConfiguration(
        "table", Collections.emptyMap()));
    HttpResponse putAllowed = executeCallback(runtimeCallbackClient, HttpMethod.PUT, wfRun,
        Constants.Service.DATASET_MANAGER, "v3/namespaces/ns1/data/datasets/" + localDatasetName,
        tableDatasetBody);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, putAllowed.getResponseCode());
    assertExchangedInternalToken(datasetHandler.capture.pollLast(), runtimeToken);
    Assert.assertEquals(tableDatasetBody, datasetHandler.lastPutBody.getAndSet(null));

    int datasetCallsBefore = datasetHandler.capture.getCallCount();
    assertCallbackForbidden(runtimeCallbackClient, HttpMethod.PUT, wfRun,
        Constants.Service.DATASET_MANAGER, "v3/namespaces/ns1/data/datasets/otherDs",
        tableDatasetBody);
    Assert.assertEquals(2L, counter("runtime.token.validation.failure.api_not_allowed"));
    Assert.assertEquals(datasetCallsBefore, datasetHandler.capture.getCallCount());

    HttpResponse deleteAllowed = executeCallback(runtimeCallbackClient, HttpMethod.DELETE, wfRun,
        Constants.Service.DATASET_MANAGER, "v3/namespaces/ns1/data/datasets/" + localDatasetName,
        null);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, deleteAllowed.getResponseCode());
    assertExchangedInternalToken(datasetHandler.capture.pollLast(), runtimeToken);

    int datasetCallsAfterAllowedDelete = datasetHandler.capture.getCallCount();
    assertCallbackForbidden(runtimeCallbackClient, HttpMethod.DELETE, wfRun,
        Constants.Service.DATASET_MANAGER, "v3/namespaces/ns1/data/datasets/otherDs", null);
    Assert.assertEquals(3L, counter("runtime.token.validation.failure.api_not_allowed"));
    Assert.assertEquals(datasetCallsAfterAllowedDelete, datasetHandler.capture.getCallCount());

    // Disallowed GET callbacks are rejected with 403 and never reach downstream services
    final int appFabricCallsBefore = appFabricHandler.capture.getCallCount();
    assertCallbackForbidden(runtimeCallbackClient, wfRun, Constants.Service.SECURE_STORE_SERVICE,
        "v3/namespaces/ns1/securekeys/sec-bad");
    Assert.assertEquals(2L, counter("runtime.token.validation.failure.secret"));

    assertCallbackForbidden(runtimeCallbackClient, wfRun, Constants.Service.APP_FABRIC_HTTP,
        "v3/namespaces/ns1/apps");
    Assert.assertEquals(4L, counter("runtime.token.validation.failure.api_not_allowed"));

    assertCallbackForbidden(legacyCallbackClient, wfRun, Constants.Service.SECURE_STORE_SERVICE,
        "v3/namespaces/ns1/securekeys/sec-ok", "legacy_token");
    Assert.assertEquals(2L, counter("runtime.token.validation.failure.legacy_token"));
    Assert.assertEquals(appFabricCallsBefore, appFabricHandler.capture.getCallCount());

    final int studioCallsBefore = studioHandler.capture.getCallCount();
    assertCallbackForbidden(runtimeCallbackClient, wfRun, STUDIO_SERVICE,
        "v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/ns1/connections/conn_bad");
    Assert.assertEquals(1L, counter("runtime.token.validation.failure.connection"));

    assertCallbackForbidden(runtimeCallbackClient, wfRun, STUDIO_SERVICE,
        "v3/namespaces/system/apps/pipeline/services/studio/methods/v1/oauth/provider/prov/credential/cred-bad");
    Assert.assertEquals(1L, counter("runtime.token.validation.failure.oauth"));
    Assert.assertEquals(studioCallsBefore, studioHandler.capture.getCallCount());

    // Legacy unscoped run with legacy INTERNAL token still succeeds under enforcing mode
    ProgramRunId legacyRun = NS1.app("legacyApp", "1.0").workflow("LegacyWf").run(RunIds.generate());
    publishRecordedStarting(legacyRun, Collections.singletonMap(
        SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc"));
    publishRecordedRunning(legacyRun);
    awaitLocalRun(legacyRun, ProgramRunStatus.RUNNING);
    assertCallbackStatus(legacyCallbackClient, legacyRun, Constants.Service.APP_FABRIC_HTTP,
        "v3/namespaces/ns1/apps", HttpURLConnection.HTTP_OK);
    Assert.assertEquals(2L, counter("runtime.token.validation.failure.legacy_token"));

    // Unauthenticated request (no X-CDAP-Runtime-Token, only Authorization Bearer) and tampered
    // legacy INTERNAL token are rejected by RuntimeIdentityHandler + AuthenticationChannelHandler
    // + InternalAccessEnforcer
    int callsBeforeAuthReject = appFabricHandler.capture.getCallCount();
    RemoteClient noRuntimeTokenClient = createCallbackClientWithBearerOnly(runtimeToken);
    assertCallbackInternalAuthError(noRuntimeTokenClient, legacyRun,
        Constants.Service.APP_FABRIC_HTTP, "v3/namespaces/ns1/apps");
    RemoteClient tamperedLegacyClient = createCallbackClient(tamperToken(legacyToken));
    assertCallbackInternalAuthError(tamperedLegacyClient, legacyRun,
        Constants.Service.APP_FABRIC_HTTP, "v3/namespaces/ns1/apps");
    Assert.assertEquals(callsBeforeAuthReject, appFabricHandler.capture.getCallCount());
  }

  private void verifyInnerChildRunsAndUrlMismatch(RemoteClient runtimeCallbackClient,
      RuntimeClient runtimeMsgClient, ProgramRunId wfRun, ProgramRunId otherRun,
      TopicId logTopic, TopicId statusTopic) throws Exception {
    // Workflow callback with -SNAPSHOT version matches wfRun ignoring version
    ProgramRunId wfSnapshotUrlRun = NS1.app("pipe1", ApplicationId.DEFAULT_VERSION)
        .workflow("DataPipelineWorkflow").run(wfRun.getRun());
    assertCallbackStatus(runtimeCallbackClient, wfSnapshotUrlRun,
        Constants.Service.SECURE_STORE_SERVICE, "v3/namespaces/ns1/securekeys/sec-ok",
        HttpURLConnection.HTTP_OK);
    assertCallbackForbidden(runtimeCallbackClient, wfSnapshotUrlRun,
        Constants.Service.SECURE_STORE_SERVICE, "v3/namespaces/ns1/securekeys/sec-bad");

    // Inner Spark child run: STARTING accepted, child callback + Spark event logs validated
    ProgramRunId sparkChildRun = NS1.app("pipe1", "1.0").spark("SparkStage").run(RunIds.generate());
    Map<String, String> childSysArgs = ImmutableMap.of(
        SystemArguments.PROFILE_NAME, NS1.profile("dp-prof").getScopedName(),
        ProgramOptionConstants.WORKFLOW_NAME, wfRun.getProgram(),
        ProgramOptionConstants.WORKFLOW_RUN_ID, wfRun.getRun(),
        ProgramOptionConstants.WORKFLOW_NODE_ID, "sparkNode");
    Message childStartingMsg = encodeStatusMessage(
        sparkChildRun, ProgramRunStatus.STARTING, childSysArgs);
    runtimeMsgClient.sendMessages(wfRun, statusTopic,
        Collections.singletonList(childStartingMsg).iterator());
    List<byte[]> childStartingPayloads = fetchNewPayloads(statusTopic);
    Assert.assertEquals(1, childStartingPayloads.size());
    Assert.assertArrayEquals(childStartingMsg.getPayload(), childStartingPayloads.get(0));

    publishRecordedStarting(sparkChildRun, childSysArgs);
    awaitLocalRun(sparkChildRun, ProgramRunStatus.STARTING);

    assertCallbackStatus(runtimeCallbackClient, sparkChildRun,
        Constants.Service.SECURE_STORE_SERVICE, "v3/namespaces/ns1/securekeys/sec-ok",
        HttpURLConnection.HTTP_OK);
    assertCallbackForbidden(runtimeCallbackClient, sparkChildRun,
        Constants.Service.SECURE_STORE_SERVICE, "v3/namespaces/ns1/securekeys/sec-bad");

    // Spark event log upload succeeds for child run with intact contents
    File eventFile = tmpFolder.newFile("spark-event.log");
    Files.write(eventFile.toPath(), "spark-events".getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.TRUNCATE_EXISTING);
    runtimeMsgClient.uploadSparkEventLogs(sparkChildRun, eventFile);
    Location eventLogsDir = locationFactory.create(
        baseConf.get(Constants.AppFabric.SPARK_EVENT_LOGS_DIR));
    List<Location> uploadedLogs = eventLogsDir.list().stream()
        .filter(loc -> loc.getName().endsWith(eventFile.getName()))
        .collect(Collectors.toList());
    Assert.assertEquals(1, uploadedLogs.size());
    try (InputStream is = uploadedLogs.get(0).getInputStream()) {
      Assert.assertEquals("spark-events",
          new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
    }

    // Inner Spark child run unknown-status wait/retry: 503 before TMS replication, 200 after
    ProgramRunId sparkChildRun2 = NS1.app("pipe1", "1.0").spark("SparkStage2")
        .run(RunIds.generate());
    Message child2RunningMsg = encodeStatusMessage(
        sparkChildRun2, ProgramRunStatus.RUNNING, Collections.emptyMap());
    try {
      runtimeMsgClient.sendMessages(wfRun, statusTopic,
          Collections.singletonList(child2RunningMsg).iterator());
      Assert.fail("Expected 503 ServiceUnavailableException for unreplicated inner run");
    } catch (ServiceUnavailableException expected) {
      // Expected wait response while waiting for inner run STARTING replication
    }
    publishRecordedStarting(sparkChildRun2, childSysArgs);
    awaitLocalRun(sparkChildRun2, ProgramRunStatus.STARTING);
    runtimeMsgClient.sendMessages(wfRun, statusTopic,
        Collections.singletonList(child2RunningMsg).iterator());
    List<byte[]> child2Payloads = fetchNewPayloads(statusTopic);
    Assert.assertEquals(1, child2Payloads.size());
    Assert.assertArrayEquals(child2RunningMsg.getPayload(), child2Payloads.get(0));

    // Cross-run URL with wfRun's token is rejected (403) across callbacks, messages, and event logs
    publishRecordedStarting(otherRun, Collections.singletonMap(
        SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc"));
    awaitLocalRun(otherRun, ProgramRunStatus.STARTING);
    assertCallbackForbidden(runtimeCallbackClient, otherRun, Constants.Service.SECURE_STORE_SERVICE,
        "v3/namespaces/ns1/securekeys/sec-ok", "url_mismatch");
    Assert.assertEquals(1L, counter("runtime.token.validation.failure.url_mismatch"));
    try {
      runtimeMsgClient.sendMessages(otherRun, logTopic,
          Collections.singletonList(encodeLogMessage(wfRun, "wrong-url")).iterator());
      Assert.fail("Expected RuntimeTokenRefusedException when sending messages to another run's URL");
    } catch (RuntimeTokenRefusedException expected) {
      Assert.assertEquals("url_mismatch", expected.getReason());
    }
    Assert.assertEquals(2L, counter("runtime.token.validation.failure.url_mismatch"));
    try {
      runtimeMsgClient.uploadSparkEventLogs(otherRun, eventFile);
      Assert.fail("Expected IOException wrapping RuntimeTokenRefusedException for Spark event log");
    } catch (IOException expected) {
      Assert.assertTrue("Expected RuntimeTokenRefusedException cause, got " + expected.getCause(),
          expected.getCause() instanceof RuntimeTokenRefusedException);
      Assert.assertEquals("url_mismatch",
          ((RuntimeTokenRefusedException) expected.getCause()).getReason());
    }
    Assert.assertEquals(3L, counter("runtime.token.validation.failure.url_mismatch"));
    Assert.assertEquals(1, eventLogsDir.list().stream()
        .filter(loc -> loc.getName().endsWith(eventFile.getName()))
        .count());

    // End-to-end RuntimeClientService fails immediately with zero retries on enforced refusal
    verifyEnforcingRuntimeClientServiceFailFast(runtimeMsgClient, otherRun, logTopic);
    Assert.assertEquals(4L, counter("runtime.token.validation.failure.url_mismatch"));
  }

  private void verifyEnforcingRuntimeClientServiceFailFast(RuntimeClient runtimeMsgClient,
      ProgramRunId otherRun, TopicId logTopic) throws Exception {
    CConfiguration clientConf = CConfiguration.copy(baseConf);
    clientConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    clientConf.set(Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX + Constants.Retry.TYPE,
        "exponential.backoff");
    clientConf.setLong(
        Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX + Constants.Retry.DELAY_BASE_MS, 50L);
    clientConf.setLong(
        Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX + Constants.Retry.DELAY_MAX_MS, 1_000L);
    clientConf.setInt(
        Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX + Constants.Retry.MAX_RETRIES,
        Integer.MAX_VALUE);
    clientConf.setLong(
        Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX + Constants.Retry.MAX_TIME_SECS, 3_600L);
    clientConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 50L);
    clientConf.setLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS, 30_000L);

    Injector clientInjector = Guice.createInjector(
        new ConfigModule(clientConf),
        new IOModule(),
        new LocalLocationModule(),
        new NoOpAuditLogModule(),
        new UserCredentialAeadEncryptionModule(),
        new AuthenticationContextModules().getNoOpModule(),
        new AuthorizationEnforcementModule().getNoOpModules(),
        new MessagingServerRuntimeModule().getInMemoryModules(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
            bind(DiscoveryService.class).toInstance(discoveryService);
            bind(RuntimeClient.class).toInstance(runtimeMsgClient);
            bind(ProgramRunId.class).toInstance(otherRun);
          }
        });

    MessagingService localMessaging = clientInjector.getInstance(MessagingService.class);
    if (localMessaging instanceof Service) {
      Services.startAndWait((Service) localMessaging);
    }
    RuntimeClientService clientService = clientInjector.getInstance(RuntimeClientService.class);
    try {
      new MultiThreadMessagingContext(localMessaging).getDirectMessagePublisher().publish(
          logTopic.getNamespace(), logTopic.getTopic(),
          encodeLogMessage(otherRun, "e2e-fail-fast").getPayload());
      Services.startAndWait(clientService);
      Tasks.waitFor(Service.State.FAILED, clientService::state, 5, TimeUnit.SECONDS);
      Throwable cause = clientService.getFailureCause();
      Assert.assertTrue("Expected RuntimeTokenRefusedException, got " + cause,
          cause instanceof RuntimeTokenRefusedException);
      Assert.assertEquals("url_mismatch", ((RuntimeTokenRefusedException) cause).getReason());
    } finally {
      try {
        Services.stopAndWait(clientService);
      } catch (Exception ignored) {
        // Expected: service already transitioned to FAILED.
      }
      if (localMessaging instanceof Service) {
        Services.stopAndWait((Service) localMessaging);
      }
    }
  }

  private void verifyEnforcingMessages(RuntimeClient runtimeMsgClient, ProgramRunId wfRun,
      ProgramRunId otherRun, TopicId logTopic, TopicId metricsTopic, TopicId statusTopic)
      throws Exception {
    receivedLogs.clear();
    Message ownLog = encodeLogMessage(wfRun, "own-log-enforcing");
    runtimeMsgClient.sendMessages(wfRun, logTopic, Arrays.asList(
        ownLog, encodeLogMessage(otherRun, "foreign-log-dropped")).iterator());
    Assert.assertEquals(1, receivedLogs.size());
    Assert.assertArrayEquals(ownLog.getPayload(), receivedLogs.get(0));
    Assert.assertEquals(1L, counter("runtime.token.validation.failure.message_logs"));

    assertSingleRelayedMessage(runtimeMsgClient, wfRun, metricsTopic,
        encodeMetricMessage(wfRun, "m.own.2", 10L),
        encodeMetricMessage(otherRun, "m.other.dropped", 20L),
        "runtime.token.validation.failure.message_metrics", 2L);

    assertSingleRelayedMessage(runtimeMsgClient, wfRun, statusTopic,
        encodeHeartbeatMessage(wfRun),
        encodeStatusMessage(otherRun, ProgramRunStatus.FAILED, Collections.emptyMap()),
        "runtime.token.validation.failure.message_status", 2L);

    assertSingleRelayedMessage(runtimeMsgClient, wfRun,
        NamespaceId.SYSTEM.topic(baseConf.get(Constants.Metadata.MESSAGING_TOPIC)),
        encodeMetadataMessage(wfRun, NS1.dataset("ownDs")),
        encodeMetadataMessage(wfRun, NS2.dataset("foreignDs")),
        "runtime.token.validation.failure.message_metadata", 1L);

    assertSingleRelayedMessage(runtimeMsgClient, wfRun,
        NamespaceId.SYSTEM.topic(baseConf.get(Constants.Audit.TOPIC)),
        encodeAuditMessage(wfRun, NS1.dataset("ownDs")),
        encodeAuditMessage(wfRun, NS2.dataset("foreignDs")),
        "runtime.token.validation.failure.message_audit", 1L);

    assertSingleRelayedMessage(runtimeMsgClient, wfRun,
        NamespaceId.SYSTEM.topic(baseConf.get(Constants.Dataset.DATA_EVENT_TOPIC)),
        encodeDataEventMessage(NS1.dataset("ownDs")),
        encodeDataEventMessage(NS2.dataset("foreignDs")),
        "runtime.token.validation.failure.message_data_event", 1L);
  }

  private void assertSingleRelayedMessage(RuntimeClient runtimeMsgClient, ProgramRunId wfRun,
      TopicId topicId, Message allowed, Message dropped, String failureMetric, long expectedCount)
      throws Exception {
    runtimeMsgClient.sendMessages(wfRun, topicId, Arrays.asList(allowed, dropped).iterator());
    List<byte[]> payloads = fetchNewPayloads(topicId);
    Assert.assertEquals(1, payloads.size());
    Assert.assertArrayEquals(allowed.getPayload(), payloads.get(0));
    Assert.assertEquals(expectedCount, counter(failureMetric));
  }

  private void verifyTamperedRuntimeToken(String runtimeToken, ProgramRunId wfRun,
      TopicId logTopic) throws Exception {
    String tamperedRuntimeToken = tamperToken(runtimeToken);
    assertCallbackStatus(createCallbackClient(tamperedRuntimeToken), wfRun,
        Constants.Service.SECURE_STORE_SERVICE, "v3/namespaces/ns1/securekeys/sec-ok",
        HttpURLConnection.HTTP_UNAUTHORIZED);
    try {
      createRuntimeClient(tamperedRuntimeToken).sendMessages(wfRun, logTopic,
          Collections.singletonList(encodeLogMessage(wfRun, "tampered")).iterator());
      Assert.fail("Expected IOException (401) for tampered RUNTIME token on RuntimeClient");
    } catch (IOException expected) {
      Assert.assertTrue(expected.getMessage().contains("401"));
    }
  }

  private void verifyColdStoreRefillAndCompletion(RemoteClient runtimeCallbackClient,
      String runtimeToken, ProgramRunId wfRun, RuntimeScopes scopes) throws Exception {
    // Cold-store active-run refill via RemoteProgramRunRecordFetcher (verifying system INTERNAL
    // token auth on the fallback fetch and local store population with preserved RUNTIME_SCOPES)
    ProgramRunId remoteOnlyRun = NS1.app("remoteApp", "1.0").workflow("RemoteWf")
        .run(RunIds.generate());
    Map<String, String> remoteOnlySysArgs = ImmutableMap.of(
        SystemArguments.PROFILE_NAME, NS1.profile("dp-prof").getScopedName(),
        SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc",
        ProgramOptionConstants.RUNTIME_SCOPES, scopes.toJson());
    appFabricHandler.remoteRuns.put(remoteOnlyRun, RunRecordDetail.builder()
        .setProgramRunId(remoteOnlyRun)
        .setStartTime(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()))
        .setStatus(ProgramRunStatus.RUNNING)
        .setSystemArgs(remoteOnlySysArgs)
        .setSourceId(new byte[] {1})
        .build());
    String remoteOnlyToken = mintToken(remoteOnlyRun, scopedOptions(remoteOnlyRun, scopes));
    RemoteClient remoteOnlyClient = createCallbackClient(remoteOnlyToken);
    int fetchesBefore = appFabricHandler.runRecordFetchCalls.get();
    assertCallbackStatus(remoteOnlyClient, remoteOnlyRun, Constants.Service.SECURE_STORE_SERVICE,
        "v3/namespaces/ns1/securekeys/sec-ok", HttpURLConnection.HTTP_OK);
    Assert.assertEquals(fetchesBefore + 1, appFabricHandler.runRecordFetchCalls.get());
    assertSystemInternalToken(appFabricHandler.lastRunRecordFetchTokenHeader.getAndSet(null),
        remoteOnlyToken);
    RunRecordDetail refilled = getLocalRun(remoteOnlyRun);
    Assert.assertNotNull(refilled);
    Assert.assertEquals(scopes.toJson(),
        refilled.getSystemArgs().get(ProgramOptionConstants.RUNTIME_SCOPES));
    assertCallbackForbidden(remoteOnlyClient, remoteOnlyRun, Constants.Service.SECURE_STORE_SERVICE,
        "v3/namespaces/ns1/securekeys/sec-bad");

    // Completing wfRun removes it from local store; fallback COMPLETED record yields 400
    appFabricHandler.remoteRuns.put(wfRun, RunRecordDetail.builder()
        .setProgramRunId(wfRun)
        .setStartTime(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()))
        .setStatus(ProgramRunStatus.COMPLETED)
        .setSourceId(new byte[] {1})
        .build());
    final int fetchesBeforeStop = appFabricHandler.runRecordFetchCalls.get();
    publishRecordedStop(wfRun, ProgramRunStatus.COMPLETED);
    awaitLocalRunRemoved(wfRun);
    Tasks.waitFor(HttpURLConnection.HTTP_BAD_REQUEST,
        () -> executeCallback(runtimeCallbackClient, wfRun, Constants.Service.SECURE_STORE_SERVICE,
            "v3/namespaces/ns1/securekeys/sec-ok").getResponseCode(),
        REPLICATION_TIMEOUT_MS, TimeUnit.MILLISECONDS, 25, TimeUnit.MILLISECONDS);
    Assert.assertTrue(appFabricHandler.runRecordFetchCalls.get() > fetchesBeforeStop);
    assertSystemInternalToken(appFabricHandler.lastRunRecordFetchTokenHeader.getAndSet(null),
        runtimeToken);
  }

  private void startMockService(String name, HttpHandler handler, String... serviceNames)
      throws Exception {
    NettyHttpService service = NettyHttpService.builder(name)
        .setHost("127.0.0.1")
        .setHttpHandlers(handler)
        .build();
    service.start();
    mockServices.add(service);
    for (String serviceName : serviceNames) {
      discoveryRegistrations.add(
          discoveryService.register(URIScheme.createDiscoverable(serviceName, service)));
    }
  }

  private RuntimeServer startRuntimeServer(boolean validationEnabled) {
    CConfiguration serverConf = CConfiguration.copy(baseConf);
    serverConf.setBoolean(Constants.RuntimeMonitor.RUNTIME_TOKEN_VALIDATION_ENABLED,
        validationEnabled);

    AccessControllerInstantiator noOpInstantiator =
        Mockito.mock(AccessControllerInstantiator.class);
    Mockito.when(noOpInstantiator.get()).thenReturn(Mockito.mock(AccessControllerSpi.class));

    Injector serverInjector = Guice.createInjector(
        new ConfigModule(serverConf, new Configuration(), SConfiguration.create()),
        new IOModule(),
        new NoOpAuditLogModule(),
        new UserCredentialAeadEncryptionModule(),
        RemoteAuthenticatorModules.getNoOpModule(),
        new AuthenticationContextModules().getMasterModule(),
        new AuthorizationEnforcementModule().getMasterModule(),
        new RuntimeServerModule() {
          @Override
          protected void bindLogProcessor() {
            bind(RemoteExecutionLogProcessor.class).toInstance(payloads ->
                payloads.forEachRemaining(receivedLogs::add));
          }
        },
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(TransactionRunner.class).toInstance(txRunner);
            bind(MessagingService.class).toInstance(messagingService);
            bind(DiscoveryService.class).toInstance(discoveryService);
            bind(DiscoveryServiceClient.class).toInstance(discoveryService);
            bind(TokenManager.class).toInstance(tokenManager);
            bind(new TypeLiteral<Codec<AccessToken>>() {
            }).toInstance(accessTokenCodec);
            bind(MetricsCollectionService.class).toInstance(countingMetricsService);
            bind(LocationFactory.class).toInstance(locationFactory);
            bind(AccessControllerInstantiator.class).toInstance(noOpInstantiator);
            bind(PermissionManager.class).to(NoOpAccessController.class);
          }
        });

    RuntimeServer server = serverInjector.getInstance(RuntimeServer.class);
    Services.startAndWait(server);
    return server;
  }

  private String mintToken(ProgramRunId runId, ProgramOptions options) throws Exception {
    Location keysDir = locationFactory.create("keys-" + keyDirCounter.incrementAndGet());
    keysDir.mkdirs();
    Location tokenFile = runnerService.generateAndSaveRuntimeToken(
        baseConf, runId, options, keysDir);
    try (InputStream is = tokenFile.getInputStream()) {
      return new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8);
    }
  }

  private ProgramOptions scopedOptions(ProgramRunId runId, RuntimeScopes scopes) {
    return new SimpleProgramOptions(
        runId.getParent(),
        new BasicArguments(ImmutableMap.of(
            SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc",
            ProgramOptionConstants.RUNTIME_SCOPES, scopes.toJson())),
        new BasicArguments());
  }

  private ProgramOptions unscopedOptions(ProgramRunId runId) {
    return new SimpleProgramOptions(
        runId.getParent(),
        new BasicArguments(Collections.singletonMap(
            SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc")),
        new BasicArguments());
  }

  private static ApplicationSpecification appSpecWithConnectionMacro(ProgramId programId,
      String connMacro) {
    Plugin plugin = new Plugin(
        Collections.emptyList(), PIPELINE_ARTIFACT,
        PluginClass.builder().setType("batchsource").setName("Src")
            .setDescription("desc").setClassName("io.cdap.Src").build(),
        PluginProperties.builder().add("connection", connMacro).build());
    WorkflowSpecification wfSpec = new WorkflowSpecification(
        "io.cdap.Wf", programId.getProgram(), "desc",
        Collections.emptyMap(), Collections.emptyList(),
        Collections.emptyMap(), Collections.emptyMap());
    return new DefaultApplicationSpecification(
        programId.getApplication(), programId.getVersion(), null, "desc", null,
        PIPELINE_ARTIFACT,
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap(programId.getProgram(), wfSpec),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        new HashMap<>(Collections.singletonMap("src", plugin)));
  }

  private RemoteClientFactory createClientFactory(String encodedToken) {
    Principal principal = new Principal(
        CONTAINER_USER,
        Principal.PrincipalType.USER,
        new Credential(encodedToken, Credential.CredentialType.INTERNAL));
    RemoteAuthenticator bearerAuth = new StaticBearerRemoteAuthenticator("external-gcp-token");
    return new RemoteClientFactory(
        discoveryService,
        new DefaultInternalAuthenticator(() -> principal),
        bearerAuth);
  }

  private RemoteClient createCallbackClient(String encodedToken) {
    return createClientFactory(encodedToken).createRemoteClient(
        Constants.Service.RUNTIME,
        DefaultHttpRequestConfig.DEFAULT,
        Constants.Gateway.INTERNAL_API_VERSION_3 + "/runtime/namespaces/");
  }

  private RemoteClient createCallbackClientWithBearerOnly(String bearerToken) {
    RemoteClientFactory factory = new RemoteClientFactory(
        discoveryService,
        setter -> { },
        new StaticBearerRemoteAuthenticator(bearerToken));
    return factory.createRemoteClient(
        Constants.Service.RUNTIME,
        DefaultHttpRequestConfig.DEFAULT,
        Constants.Gateway.INTERNAL_API_VERSION_3 + "/runtime/namespaces/");
  }

  private RuntimeClient createRuntimeClient(String encodedToken) {
    return new RuntimeClient(baseConf, createClientFactory(encodedToken));
  }

  private String callbackPath(ProgramRunId urlRunId, String targetService, String targetPath) {
    return String.format("%s/apps/%s/versions/%s/%s/%s/runs/%s/services/%s/%s",
        urlRunId.getNamespace(), urlRunId.getApplication(), urlRunId.getVersion(),
        urlRunId.getType().getCategoryName(), urlRunId.getProgram(), urlRunId.getRun(),
        targetService, targetPath);
  }

  private HttpResponse executeCallback(RemoteClient client, ProgramRunId urlRunId,
      String targetService, String targetPath) throws Exception {
    return executeCallback(client, HttpMethod.GET, urlRunId, targetService, targetPath, null);
  }

  private HttpResponse executeCallback(RemoteClient client, HttpMethod method,
      ProgramRunId urlRunId, String targetService, String targetPath, @Nullable String body)
      throws Exception {
    HttpRequest.Builder builder = client.requestBuilder(
        method, callbackPath(urlRunId, targetService, targetPath));
    if (body != null) {
      builder.withBody(body);
    }
    return client.execute(builder.build());
  }

  private void assertCallbackStatus(RemoteClient client, ProgramRunId urlRunId,
      String targetService, String targetPath, int expectedStatus) throws Exception {
    HttpResponse response = executeCallback(client, urlRunId, targetService, targetPath);
    Assert.assertEquals(expectedStatus, response.getResponseCode());
  }

  private void assertCallbackForbidden(RemoteClient client, ProgramRunId urlRunId,
      String targetService, String targetPath) throws Exception {
    assertCallbackForbidden(client, HttpMethod.GET, urlRunId, targetService, targetPath, null,
        null);
  }

  private void assertCallbackForbidden(RemoteClient client, ProgramRunId urlRunId,
      String targetService, String targetPath, @Nullable String expectedFailureHeader)
      throws Exception {
    assertCallbackForbidden(client, HttpMethod.GET, urlRunId, targetService, targetPath, null,
        expectedFailureHeader);
  }

  private void assertCallbackForbidden(RemoteClient client, HttpMethod method,
      ProgramRunId urlRunId, String targetService, String targetPath, @Nullable String body)
      throws Exception {
    assertCallbackForbidden(client, method, urlRunId, targetService, targetPath, body, null);
  }

  private void assertCallbackForbidden(RemoteClient client, HttpMethod method,
      ProgramRunId urlRunId, String targetService, String targetPath, @Nullable String body,
      @Nullable String expectedFailureHeader) throws Exception {
    HttpURLConnection conn = client.openConnection(
        method, callbackPath(urlRunId, targetService, targetPath));
    try {
      if (body != null) {
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        try (OutputStream os = conn.getOutputStream()) {
          os.write(body.getBytes(StandardCharsets.UTF_8));
        }
      }
      Assert.assertEquals("Expected HTTP 403 for " + targetService + "/" + targetPath,
          HttpURLConnection.HTTP_FORBIDDEN, conn.getResponseCode());
      Assert.assertEquals("Unexpected X-CDAP-Runtime-Token-Failure header for " + targetService
              + "/" + targetPath,
          expectedFailureHeader,
          conn.getHeaderField(Constants.Security.Headers.RUNTIME_TOKEN_FAILURE));
    } finally {
      conn.disconnect();
    }
  }

  private void assertCallbackInternalAuthError(RemoteClient client, ProgramRunId urlRunId,
      String targetService, String targetPath) throws Exception {
    try {
      HttpResponse response = client.execute(
          client.requestBuilder(HttpMethod.GET,
              callbackPath(urlRunId, targetService, targetPath)).build(),
          Idempotency.NONE);
      Assert.fail("Expected ServiceException from InternalAccessEnforcer, got HTTP "
          + response.getResponseCode());
    } catch (ServiceException expected) {
      Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, expected.getStatusCode());
    }
  }

  private void assertExchangedInternalToken(@Nullable CapturedRequest captured,
      String incomingRuntimeToken) throws Exception {
    Assert.assertNotNull("Expected downstream request to be captured", captured);
    Assert.assertEquals(Constants.Security.Authentication.RUNTIME_IDENTITY, captured.userIdHeader);
    Assert.assertNull("Authorization header must be stripped on forwarded request",
        captured.authorizationHeader);
    assertSystemInternalToken(captured.runtimeTokenHeader, incomingRuntimeToken);
  }

  private void assertSystemInternalToken(@Nullable String tokenHeaderValue,
      String incomingRuntimeToken) throws Exception {
    Assert.assertNotNull("Expected forwarded X-CDAP-Runtime-Token header", tokenHeaderValue);
    String prefix = Credential.CREDENTIAL_TYPE_INTERNAL + " ";
    Assert.assertTrue("Expected CDAP-Internal prefix: " + tokenHeaderValue,
        tokenHeaderValue.startsWith(prefix));
    String forwardedBase64 = tokenHeaderValue.substring(prefix.length()).trim();
    Assert.assertNotEquals(
        "Forwarded token must be an exchanged token, not the incoming RUNTIME token",
        incomingRuntimeToken, forwardedBase64);

    AccessToken decoded = accessTokenCodec.decode(Base64.getDecoder().decode(forwardedBase64));
    tokenManager.validateSecret(decoded);
    UserIdentity identity = decoded.getIdentifier();
    Assert.assertEquals(UserIdentity.IdentifierType.INTERNAL, identity.getIdentifierType());
    long ttlMs = identity.getExpireTimestamp() - identity.getIssueTimestamp();
    Assert.assertEquals(EXCHANGE_TOKEN_TTL_MS, ttlMs);
  }

  private String tamperToken(String base64Token) {
    byte[] bytes = Base64.getDecoder().decode(base64Token);
    bytes[bytes.length - 1] ^= 0x01;
    return Base64.getEncoder().encodeToString(bytes);
  }

  private void publishRecordedStarting(ProgramRunId runId, Map<String, String> systemArgs)
      throws Exception {
    Map<String, String> fullSystemArgs = new HashMap<>(systemArgs);
    fullSystemArgs.putIfAbsent(SystemArguments.PROFILE_NAME,
        NS1.profile("dp-prof").getScopedName());
    Map<String, String> props = new HashMap<>();
    props.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(runId));
    props.put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.STARTING.name());
    props.put(ProgramOptionConstants.SYSTEM_OVERRIDES, GSON.toJson(fullSystemArgs));
    props.put(ProgramOptionConstants.USER_OVERRIDES, GSON.toJson(Collections.emptyMap()));
    publishRecordEvent(props);
  }

  private void publishRecordedRunning(ProgramRunId runId) throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(runId));
    props.put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.RUNNING.name());
    props.put(ProgramOptionConstants.LOGICAL_START_TIME, String.valueOf(System.currentTimeMillis()));
    publishRecordEvent(props);
  }

  private void publishRecordedStop(ProgramRunId runId, ProgramRunStatus status) throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(runId));
    props.put(ProgramOptionConstants.PROGRAM_STATUS, status.name());
    props.put(ProgramOptionConstants.END_TIME, String.valueOf(System.currentTimeMillis()));
    publishRecordEvent(props);
  }

  private void publishRecordEvent(Map<String, String> props) throws Exception {
    String topic = baseConf.get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC);
    Notification notification = new Notification(Notification.Type.PROGRAM_STATUS, props);
    messagingContext.getMessagePublisher().publish(
        NamespaceId.SYSTEM.getNamespace(), topic, GSON.toJson(notification));
  }

  private void awaitLocalRun(ProgramRunId runId, ProgramRunStatus expectedStatus) throws Exception {
    Tasks.waitFor(expectedStatus, () -> {
      RunRecordDetail detail = getLocalRun(runId);
      return detail == null ? null : detail.getStatus();
    }, REPLICATION_TIMEOUT_MS, TimeUnit.MILLISECONDS, 25, TimeUnit.MILLISECONDS);
  }

  private void awaitLocalRunRemoved(ProgramRunId runId) throws Exception {
    Tasks.waitFor(true, () -> getLocalRun(runId) == null,
        REPLICATION_TIMEOUT_MS, TimeUnit.MILLISECONDS, 25, TimeUnit.MILLISECONDS);
  }

  @Nullable
  private RunRecordDetail getLocalRun(ProgramRunId runId) {
    return TransactionRunners.run(txRunner,
        (TxCallable<RunRecordDetail>) context -> AppMetadataStore.create(context).getRun(runId));
  }

  private long counter(String metricName) {
    return counters.getOrDefault(metricName, 0L);
  }

  private void createTopics(String... topicNames) throws Exception {
    for (String topic : topicNames) {
      try {
        messagingService.createTopic(new DefaultTopicMetadata(NamespaceId.SYSTEM.topic(topic)));
      } catch (TopicAlreadyExistsException expected) {
        // Ignore if already created
      }
    }
  }

  private List<byte[]> fetchNewPayloads(TopicId topicId) throws Exception {
    String cursor = topicCursors.get(topicId);
    List<byte[]> payloads = new ArrayList<>();
    String lastId = cursor;
    try (CloseableIterator<Message> it = messagingContext.getMessageFetcher().fetch(
        topicId.getNamespace(), topicId.getTopic(), 100, cursor)) {
      while (it.hasNext()) {
        Message msg = it.next();
        payloads.add(msg.getPayload());
        lastId = msg.getId();
      }
    }
    if (lastId != null) {
      topicCursors.put(topicId, lastId);
    }
    return payloads;
  }

  private Message encodeLogMessage(ProgramRunId runId, String text) {
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(
        runId, Collections.emptyMap());
    LoggerContext loggerContext = new LoggerContext();
    LoggingEvent event = new LoggingEvent("io.cdap.test", loggerContext.getLogger("test"),
        Level.INFO, text, null, null);
    byte[] encoded = new LoggingEventSerializer().toBytes(new LogMessage(event, loggingContext));
    return toMessage(encoded);
  }

  private Message encodeMetricMessage(ProgramRunId runId, String name, long value)
      throws Exception {
    Map<String, String> tags = new HashMap<>();
    tags.put(Constants.Metrics.Tag.NAMESPACE, runId.getNamespace());
    tags.put(Constants.Metrics.Tag.APP, runId.getApplication());
    tags.put(Constants.Metrics.Tag.WORKFLOW, runId.getProgram());
    tags.put(Constants.Metrics.Tag.RUN_ID, runId.getRun());
    MetricValues metricValues = new MetricValues(
        tags, name, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
        value, MetricType.COUNTER);
    Schema schema = new ReflectionSchemaGenerator().generate(MetricValues.class);
    DatumWriter<MetricValues> writer = new ASMDatumWriterFactory(
        new ASMFieldAccessorFactory()).create(TypeToken.of(MetricValues.class), schema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.encode(metricValues, new BinaryEncoder(out));
    return toMessage(out.toByteArray());
  }

  private Message encodeHeartbeatMessage(ProgramRunId runId) {
    Map<String, String> props = new HashMap<>();
    props.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(runId));
    props.put(ProgramOptionConstants.HEART_BEAT_TIME, String.valueOf(System.currentTimeMillis()));
    return toMessage(GSON.toJson(new Notification(Notification.Type.PROGRAM_HEART_BEAT, props))
        .getBytes(StandardCharsets.UTF_8));
  }

  private Message encodeStatusMessage(ProgramRunId runId, ProgramRunStatus status,
      Map<String, String> systemArgs) {
    Map<String, String> props = new HashMap<>();
    props.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(runId));
    props.put(ProgramOptionConstants.PROGRAM_STATUS, status.name());
    if (status == ProgramRunStatus.STARTING) {
      props.put(ProgramOptionConstants.PROGRAM_ARTIFACT_ID,
          GSON.toJson(runId.getNamespaceId().artifact("cdap-data-pipeline", "6.12.0")));
    } else if (status == ProgramRunStatus.RUNNING) {
      props.put(ProgramOptionConstants.LOGICAL_START_TIME,
          String.valueOf(System.currentTimeMillis()));
    } else if (status == ProgramRunStatus.COMPLETED
        || status == ProgramRunStatus.FAILED
        || status == ProgramRunStatus.KILLED) {
      props.put(ProgramOptionConstants.END_TIME,
          String.valueOf(System.currentTimeMillis()));
    }
    if (!systemArgs.isEmpty()) {
      props.put(ProgramOptionConstants.SYSTEM_OVERRIDES, GSON.toJson(systemArgs));
    }
    return toMessage(GSON.toJson(new Notification(Notification.Type.PROGRAM_STATUS, props))
        .getBytes(StandardCharsets.UTF_8));
  }

  private Message encodeMetadataMessage(ProgramRunId emitterRunId, DatasetId datasetId) {
    MetadataMessage message = new MetadataMessage(
        MetadataMessage.Type.USAGE, emitterRunId,
        GSON.toJsonTree(Collections.singletonMap("datasetId", datasetId)));
    return toMessage(GSON.toJson(message).getBytes(StandardCharsets.UTF_8));
  }

  private Message encodeAuditMessage(ProgramRunId accessorRunId, DatasetId datasetId) {
    AuditMessage audit = new AuditMessage(
        System.currentTimeMillis(), datasetId, "user1", AuditType.ACCESS,
        new AccessPayload(AccessType.READ, accessorRunId));
    return toMessage(GSON.toJson(audit).getBytes(StandardCharsets.UTF_8));
  }

  private Message encodeDataEventMessage(DatasetId datasetId) {
    Notification notification = Notification.forPartitions(datasetId, Collections.emptyList());
    return toMessage(GSON.toJson(notification).getBytes(StandardCharsets.UTF_8));
  }

  private static Message toMessage(byte[] payload) {
    return new Message() {
      private final String id = RunIds.generate().getId();

      @Override
      public String getId() {
        return id;
      }

      @Override
      public byte[] getPayload() {
        return payload;
      }
    };
  }

  private static MetricsCollectionService createCountingMetricsService(
      Map<String, Long> counters) {
    return new NoOpMetricsCollectionService() {
      @Override
      public MetricsContext getContext(Map<String, String> tags) {
        return new MetricsContext() {
          @Override
          public void increment(String metricName, long value) {
            counters.merge(metricName, value, Long::sum);
          }

          @Override
          public void gauge(String metricName, long value) {
          }

          @Override
          public void event(String metricName, long value) {
          }

          @Override
          public MetricsContext childContext(Map<String, String> childTags) {
            return this;
          }

          @Override
          public MetricsContext childContext(String tagName, String tagValue) {
            return this;
          }

          @Override
          public Map<String, String> getTags() {
            return tags;
          }
        };
      }
    };
  }

  private static void stopQuietly(ThrowingRunnable action) {
    try {
      action.run();
    } catch (Exception ignored) {
      // Continue tearing down remaining test resources
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private static final class StaticBearerRemoteAuthenticator implements RemoteAuthenticator {

    private final Credential credential;

    private StaticBearerRemoteAuthenticator(String token) {
      this.credential = new Credential(token, Credential.CredentialType.EXTERNAL_BEARER);
    }

    @Override
    public String getName() {
      return "test-bearer";
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

  private static final class CapturedRequest {

    @Nullable
    private final String runtimeTokenHeader;
    @Nullable
    private final String userIdHeader;
    @Nullable
    private final String authorizationHeader;

    private CapturedRequest(@Nullable String runtimeTokenHeader, @Nullable String userIdHeader,
        @Nullable String authorizationHeader) {
      this.runtimeTokenHeader = runtimeTokenHeader;
      this.userIdHeader = userIdHeader;
      this.authorizationHeader = authorizationHeader;
    }
  }

  private static final class CallbackCapture {

    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicReference<CapturedRequest> lastRequest = new AtomicReference<>();

    private void record(io.netty.handler.codec.http.HttpRequest request) {
      calls.incrementAndGet();
      lastRequest.set(new CapturedRequest(
          request.headers().get(Constants.Security.Headers.RUNTIME_TOKEN),
          request.headers().get(Constants.Security.Headers.USER_ID),
          request.headers().get(HttpHeaderNames.AUTHORIZATION)));
    }

    private int getCallCount() {
      return calls.get();
    }

    @Nullable
    private CapturedRequest pollLast() {
      return lastRequest.getAndSet(null);
    }
  }

  /**
   * Mock downstream App-Fabric and secure-store handler recording forwarded callback headers and
   * serving fallback run records.
   */
  public static final class MockAppFabricHandler extends AbstractHttpHandler {

    private final CallbackCapture capture = new CallbackCapture();
    private final AtomicInteger runRecordFetchCalls = new AtomicInteger();
    private final AtomicReference<String> lastRunRecordFetchTokenHeader = new AtomicReference<>();
    private final Map<ProgramRunId, RunRecordDetail> remoteRuns = new ConcurrentHashMap<>();

    @GET
    @Path("/v3/namespaces/{namespace}/securekeys/{key}")
    public void getSecret(io.netty.handler.codec.http.HttpRequest request, HttpResponder responder,
        @PathParam("namespace") String namespace, @PathParam("key") String key) {
      capture.record(request);
      responder.sendJson(HttpResponseStatus.OK, "{\"name\":\"" + key + "\"}");
    }

    @GET
    @Path("/v3/namespaces/{namespace}/apps")
    public void listApps(io.netty.handler.codec.http.HttpRequest request, HttpResponder responder,
        @PathParam("namespace") String namespace) {
      capture.record(request);
      responder.sendJson(HttpResponseStatus.OK, "[]");
    }

    @GET
    @Path(Constants.Gateway.INTERNAL_API_VERSION_3
        + "/namespaces/{namespace}/apps/{app}/versions/{version}/{program-type}/{program}/runs/{run}")
    public void getRunRecord(io.netty.handler.codec.http.HttpRequest request,
        HttpResponder responder,
        @PathParam("namespace") String namespace, @PathParam("app") String app,
        @PathParam("version") String version, @PathParam("program-type") String programType,
        @PathParam("program") String program, @PathParam("run") String run) {
      runRecordFetchCalls.incrementAndGet();
      lastRunRecordFetchTokenHeader.set(
          request.headers().get(Constants.Security.Headers.RUNTIME_TOKEN));
      ProgramRunId runId = new NamespaceId(namespace).app(app, version)
          .program(ProgramType.valueOfCategoryName(programType), program).run(run);
      RunRecordDetail detail = remoteRuns.get(runId);
      if (detail == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(detail));
    }
  }

  /**
   * Mock downstream pipeline studio handler serving connection properties (for both launch-time
   * scope extraction and runtime callbacks) and OAuth credentials.
   */
  public static final class MockStudioHandler extends AbstractHttpHandler {

    private final CallbackCapture capture = new CallbackCapture();

    @GET
    @Path("/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/{ns}/connections/{conn}")
    public void getConnection(io.netty.handler.codec.http.HttpRequest request,
        HttpResponder responder,
        @PathParam("ns") String namespace, @PathParam("conn") String conn) {
      capture.record(request);
      responder.sendJson(HttpResponseStatus.OK,
          "{\"name\":\"" + conn + "\",\"plugin\":{\"properties\":{"
              + "\"secret\":\"${secure(sec-ok)}\","
              + "\"token\":\"${oauth(prov,cred-ok)}\"}}}");
    }

    @GET
    @Path("/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/oauth/provider/{prov}/credential/{cred}")
    public void getOauthCredential(io.netty.handler.codec.http.HttpRequest request,
        HttpResponder responder,
        @PathParam("prov") String provider, @PathParam("cred") String credential) {
      capture.record(request);
      responder.sendJson(HttpResponseStatus.OK, "{\"accessToken\":\"tok\"}");
    }
  }

  /**
   * Mock downstream dataset manager handler recording forwarded headers and PUT payloads.
   */
  public static final class MockDatasetHandler extends AbstractHttpHandler {

    private final CallbackCapture capture = new CallbackCapture();
    private final AtomicReference<String> lastPutBody = new AtomicReference<>();

    @PUT
    @Path("/v3/namespaces/{namespace}/data/datasets/{name}")
    public void createDataset(FullHttpRequest request, HttpResponder responder,
        @PathParam("namespace") String namespace, @PathParam("name") String name) {
      capture.record(request);
      lastPutBody.set(request.content().toString(StandardCharsets.UTF_8));
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @DELETE
    @Path("/v3/namespaces/{namespace}/data/datasets/{name}")
    public void deleteDataset(io.netty.handler.codec.http.HttpRequest request,
        HttpResponder responder,
        @PathParam("namespace") String namespace, @PathParam("name") String name) {
      capture.record(request);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }
}
