/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.GoneException;
import io.cdap.cdap.common.HttpExceptionHandler;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.http.AuthenticationChannelHandler;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.NoOpRemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.service.Services;
import io.cdap.cdap.common.utils.TimeProvider;
import io.cdap.cdap.data.runtime.ConstantTransactionSystemClient;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.logging.gateway.handlers.ProgramRunRecordFetcher;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordFetcher;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.proto.ProgramRunCluster;
import io.cdap.cdap.proto.ProgramRunClusterStatus;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.InternalAccessEnforcer;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.NoOpAccessController;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.concurrent.EventExecutor;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

/**
 * Unit tests for {@link DirectRuntimeRequestValidator}.
 */
@RunWith(MockitoJUnitRunner.class)
public class DirectRuntimeRequestValidatorTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final ArtifactId ARTIFACT_ID =
      new ArtifactId("test", new ArtifactVersion("1.0"), ArtifactScope.USER);
  private static final Gson GSON = new Gson();
  private static final RuntimeScopes SAMPLE_SCOPES = new RuntimeScopes(
      Collections.singletonList("conn1"),
      Collections.singletonList("prov/cred"),
      Collections.singletonList("sec1"),
      false);

  private CConfiguration cConf;
  private TransactionRunner txRunner;
  private TokenManager tokenManager;
  private Codec<AccessToken> accessTokenCodec;

  @Mock
  private AccessEnforcer accessEnforcer;
  @Mock
  private AuthenticationContext authenticationContext;

  @Before
  public void setup() throws IOException, TableAlreadyExistsException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().toString());
    // Default test TTL is 0; cache-specific tests set a positive POLL_TIME_MS explicitly.
    cConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 0L);

    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        new IOModule(),
        new SystemDatasetRuntimeModule().getInMemoryModules(),
        new DataSetsModules().getInMemoryModules(),
        new NamespaceAdminTestModule(),
        new StorageModule(),
        new AuthenticationContextModules().getNoOpModule(),
        new CoreSecurityRuntimeModule().getInMemoryModules(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
            bind(AccessEnforcer.class).to(NoOpAccessController.class);
            bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class);
            bind(Store.class).to(DefaultStore.class);
          }
        }
    );

    // Create store definition
    StoreDefinition.AppMetadataStore.create(injector.getInstance(StructuredTableAdmin.class));

    txRunner = injector.getInstance(TransactionRunner.class);
    tokenManager = injector.getInstance(TokenManager.class);
    Services.startAndWait(tokenManager);
    accessTokenCodec = injector.getInstance(Key.get(new TypeLiteral<Codec<AccessToken>>() { }));
  }

  @After
  public void cleanup() {
    if (tokenManager != null) {
      Services.stopAndWait(tokenManager);
    }
    // Clears the StructuredTableRegistry backed by InMemoryTableService (singleton per JVM).
    InMemoryTableService.reset();
    SecurityRequestContext.reset();
  }

  // ---- Existing baseline tests (internal auth off) ----

  @Test
  public void testValid() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").spark("spark")
        .run(RunIds.generate());
    insertLocalStartingRun(programRunId, Collections.emptyMap());

    RuntimeRequestValidator validator = createValidator(cConf, new MockProgramRunRecordFetcher(),
        new RecordingMetricsCollectionService());
    ValidatedRuntimeRequest validated = validator.validate(programRunId, newGetRequest("/"));
    Assert.assertFalse(validated.isRuntimeToken());
    Assert.assertNull(validated.getTokenRunId());
    Assert.assertNull(validated.getRuntimeScopes());
    Assert.assertEquals(ProgramRunStatus.STARTING,
        validated.getProgramRunInfo().getProgramRunStatus());
  }

  @Test(expected = BadRequestException.class)
  public void testInvalid() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").spark("spark")
        .run(RunIds.generate());
    RuntimeRequestValidator validator = createValidator(cConf, new MockProgramRunRecordFetcher(),
        new RecordingMetricsCollectionService());
    validator.validate(programRunId, newGetRequest("/"));
  }

  @Test(expected = UnauthorizedException.class)
  public void testUnauthorized() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").spark("spark")
        .run(RunIds.generate());
    RuntimeRequestValidator validator = createValidator(cConf, new MockProgramRunRecordFetcher(),
        new RecordingMetricsCollectionService());

    Principal principal = new Principal("test", Principal.PrincipalType.USER);
    Mockito.when(authenticationContext.getPrincipal()).thenReturn(principal);
    Mockito.doThrow(new UnauthorizedException("Unauthorized"))
        .when(accessEnforcer).enforce(programRunId, principal, StandardPermission.GET);

    validator.validate(programRunId, newGetRequest("/"));
  }

  @Test(expected = GoneException.class)
  public void testNotRunning() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").spark("spark")
        .run(RunIds.generate());
    insertLocalCompletedRun(programRunId, Collections.emptyMap(), ProgramRunStatus.COMPLETED);

    RuntimeRequestValidator validator = createValidator(cConf, new MockProgramRunRecordFetcher(),
        new RecordingMetricsCollectionService());
    validator.validate(programRunId, newGetRequest("/"));
  }

  @Test
  public void testFetcher() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").spark("spark")
        .run(RunIds.generate());
    RunRecordDetail runRecord = createRunRecord(programRunId, ProgramRunStatus.RUNNING,
        ProgramRunClusterStatus.PROVISIONED, Collections.emptyMap());

    MockProgramRunRecordFetcher runRecordFetcher = new MockProgramRunRecordFetcher()
        .putRunRecord(runRecord);
    RuntimeRequestValidator validator = createValidator(cConf, runRecordFetcher,
        new RecordingMetricsCollectionService());

    // First call hits fetcher
    ProgramRunInfo programRunInfo = validator.validate(programRunId, newGetRequest("/"))
        .getProgramRunInfo();
    Assert.assertEquals(ProgramRunStatus.RUNNING, programRunInfo.getProgramRunStatus());
    Assert.assertEquals(1, runRecordFetcher.getFetchCount(programRunId));

    // Second call hits local runtime store even after fetcher clears record
    runRecordFetcher.clear();
    programRunInfo = validator.validate(programRunId, newGetRequest("/")).getProgramRunInfo();
    Assert.assertEquals(ProgramRunStatus.RUNNING, programRunInfo.getProgramRunStatus());
  }

  @Test
  public void testValidProgramInStoppingState() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").spark("spark")
        .run(RunIds.generate());
    long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    long terminateTs = now + 300L;
    TransactionRunners.run(txRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      store.recordProgramProvisioning(programRunId, Collections.emptyMap(),
          Collections.singletonMap(SystemArguments.PROFILE_NAME, "system:default"),
          createSourceId(1), ARTIFACT_ID);
      store.recordProgramProvisioned(programRunId, 1, createSourceId(2));
      store.recordProgramStart(programRunId, null, Collections.emptyMap(), createSourceId(3));
      store.recordProgramRunning(programRunId, System.currentTimeMillis(), null, createSourceId(4));
      store.recordProgramStopping(programRunId, createSourceId(5), now, terminateTs);
    });

    RuntimeRequestValidator validator = createValidator(cConf, new MockProgramRunRecordFetcher(),
        new RecordingMetricsCollectionService());
    ProgramRunInfo programRunInfo = validator.validate(programRunId, newGetRequest("/"))
        .getProgramRunInfo();
    Assert.assertEquals(ProgramRunStatus.STOPPING, programRunInfo.getProgramRunStatus());
    Assert.assertEquals(terminateTs, programRunInfo.getTerminateTimestamp());
  }

  // ---- Internal auth off with RUNTIME token header ----

  @Test
  public void testInternalAuthOffIgnoresRuntimeTokenAndUsesLegacyPath() throws Exception {
    CConfiguration conf = createSecurityConf(false, true, 60_000L);
    ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").spark("spark")
        .run(RunIds.generate());
    insertLocalStartingRun(runId,
        Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson()));

    RecordingMetricsCollectionService metrics = new RecordingMetricsCollectionService();
    RuntimeRequestValidator validator = createValidator(conf, new MockProgramRunRecordFetcher(),
        metrics);
    HttpRequest request = createRuntimeTokenRequest(runId);

    ValidatedRuntimeRequest validated = validator.validate(runId, request);
    Assert.assertFalse(validated.isRuntimeToken());
    Mockito.verify(accessEnforcer).enforce(runId, null, StandardPermission.GET);
    Assert.assertTrue(metrics.getCounts().isEmpty());
  }

  // ---- Decision table: Legacy token with internal auth on ----

  @Test
  public void testLegacyEnforcerRejectionPerformsZeroLookups() throws Exception {
    CConfiguration conf = createSecurityConf(true, true, 60_000L);
    ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").spark("spark")
        .run(RunIds.generate());
    MockProgramRunRecordFetcher fetcher = new MockProgramRunRecordFetcher();
    Principal principal = new Principal("runtime", Principal.PrincipalType.USER);
    Mockito.when(authenticationContext.getPrincipal()).thenReturn(principal);
    Mockito.doThrow(new UnauthorizedException("denied"))
        .when(accessEnforcer).enforce(runId, principal, StandardPermission.GET);

    RuntimeRequestValidator validator = createValidator(conf, fetcher,
        new RecordingMetricsCollectionService());
    try {
      validator.validate(runId, createInternalTokenRequest("runtime"));
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException expected) {
      // expected
    }
    Assert.assertEquals(0, fetcher.getTotalFetchCount());
  }

  @Test
  public void testLegacyTokenForUnscopedRunAllowedInBothModes() throws Exception {
    ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").spark("spark")
        .run(RunIds.generate());
    insertLocalStartingRun(runId, Collections.emptyMap());

    for (boolean enforcing : new boolean[]{false, true}) {
      RecordingMetricsCollectionService metrics = new RecordingMetricsCollectionService();
      RuntimeRequestValidator validator = createValidator(
          createSecurityConf(true, enforcing, 60_000L), new MockProgramRunRecordFetcher(), metrics);
      ValidatedRuntimeRequest result = validator.validate(runId,
          createInternalTokenRequest("runtime"));
      Assert.assertFalse(result.isRuntimeToken());
      Assert.assertTrue(metrics.getCounts().isEmpty());
    }
  }

  @Test
  public void testLegacyTokenForScopedRunReportOnlyAndEnforcing() throws Exception {
    ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf")
        .run(RunIds.generate());
    RunRecordDetail record = createRunRecord(runId, ProgramRunStatus.RUNNING,
        ProgramRunClusterStatus.PROVISIONED,
        Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson()));

    // Report-only: counts legacy_token and allows, with one lookup
    MockProgramRunRecordFetcher fetcherReportOnly = new MockProgramRunRecordFetcher()
        .putRunRecord(record);
    RecordingMetricsCollectionService metricsReportOnly = new RecordingMetricsCollectionService();
    RuntimeRequestValidator validatorReportOnly = createValidator(
        createSecurityConf(true, false, 60_000L), fetcherReportOnly, metricsReportOnly);
    ValidatedRuntimeRequest result = validatorReportOnly.validate(runId,
        createInternalTokenRequest("runtime"));
    Assert.assertFalse(result.isRuntimeToken());
    Assert.assertEquals(1, fetcherReportOnly.getTotalFetchCount());
    Assert.assertEquals(Long.valueOf(1L),
        metricsReportOnly.getCount(RuntimeValidationFailure.LEGACY_TOKEN));

    // Enforcing: runs enforcer first, one lookup, counts legacy_token, throws 403
    InMemoryTableService.reset();
    setupStoreAgain();
    MockProgramRunRecordFetcher fetcherEnforcing = new MockProgramRunRecordFetcher()
        .putRunRecord(record);
    RecordingMetricsCollectionService metricsEnforcing = new RecordingMetricsCollectionService();
    RuntimeRequestValidator validatorEnforcing = createValidator(
        createSecurityConf(true, true, 60_000L), fetcherEnforcing, metricsEnforcing);
    Principal principal = new Principal("runtime", Principal.PrincipalType.USER);
    Mockito.when(authenticationContext.getPrincipal()).thenReturn(principal);
    HttpRequest internalReq = createInternalTokenRequest("runtime");
    RuntimeTokenRefusedException thrown = Assert.assertThrows(
        RuntimeTokenRefusedException.class,
        () -> validatorEnforcing.validate(runId, internalReq));
    Assert.assertEquals(RuntimeValidationFailure.LEGACY_TOKEN.getReason(), thrown.getReason());
    Mockito.verify(accessEnforcer).enforce(runId, principal, StandardPermission.GET);
    Assert.assertEquals(1, fetcherEnforcing.getTotalFetchCount());
    Assert.assertEquals(Long.valueOf(1L),
        metricsEnforcing.getCount(RuntimeValidationFailure.LEGACY_TOKEN));
  }

  @Test
  public void testLegacyTokenForMalformedScopesRunReportOnlyAndEnforcing() throws Exception {
    ProgramRunId badScopesRun = NamespaceId.DEFAULT.app("app", "1.0").workflow("badScopesWf")
        .run(RunIds.generate());
    insertLocalStartingRun(badScopesRun,
        Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, "{not-valid-json"));

    // Report-only: malformed runtime.scopes still counts as scoped and records LEGACY_TOKEN
    RecordingMetricsCollectionService metricsReportOnly = new RecordingMetricsCollectionService();
    RuntimeRequestValidator validatorReportOnly = createValidator(
        createSecurityConf(true, false, 60_000L), new MockProgramRunRecordFetcher(),
        metricsReportOnly);
    ValidatedRuntimeRequest roResult = validatorReportOnly.validate(badScopesRun,
        createInternalTokenRequest("runtime"));
    Assert.assertFalse(roResult.isRuntimeToken());
    Assert.assertEquals(Long.valueOf(1L),
        metricsReportOnly.getCount(RuntimeValidationFailure.LEGACY_TOKEN));

    // Enforcing: malformed runtime.scopes still rejects legacy INTERNAL tokens with 403
    RecordingMetricsCollectionService metricsEnforcing = new RecordingMetricsCollectionService();
    RuntimeRequestValidator validatorEnforcing = createValidator(
        createSecurityConf(true, true, 60_000L), new MockProgramRunRecordFetcher(),
        metricsEnforcing);
    HttpRequest internalReq = createInternalTokenRequest("runtime");
    RuntimeTokenRefusedException thrown = Assert.assertThrows(
        RuntimeTokenRefusedException.class,
        () -> validatorEnforcing.validate(badScopesRun, internalReq));
    Assert.assertEquals(RuntimeValidationFailure.LEGACY_TOKEN.getReason(), thrown.getReason());
    Assert.assertEquals(Long.valueOf(1L),
        metricsEnforcing.getCount(RuntimeValidationFailure.LEGACY_TOKEN));
  }

  @Test
  public void testLegacyTokenForInnerRunOfScopedWorkflowChecksParentScopes() throws Exception {
    ApplicationId appId = NamespaceId.DEFAULT.app("app", "1.0");
    ProgramRunId wfRun = appId.workflow("wf").run(RunIds.generate());
    ProgramRunId sparkRun = appId.spark("innerSpark").run(RunIds.generate());

    insertLocalStartingRun(wfRun,
        Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson()));
    insertLocalStartingRun(sparkRun, ImmutableMap.of(
        ProgramOptionConstants.WORKFLOW_NAME, wfRun.getProgram(),
        ProgramOptionConstants.WORKFLOW_RUN_ID, wfRun.getRun(),
        ProgramOptionConstants.WORKFLOW_NODE_ID, "sparkNode"));

    // Report-only: inner run itself has no scopes, top-level run wfRun has scopes -> count and allow
    RecordingMetricsCollectionService metricsReportOnly = new RecordingMetricsCollectionService();
    RuntimeRequestValidator validatorReportOnly = createValidator(
        createSecurityConf(true, false, 60_000L), new MockProgramRunRecordFetcher(),
        metricsReportOnly);
    ValidatedRuntimeRequest roResult = validatorReportOnly.validate(sparkRun,
        createInternalTokenRequest("runtime"));
    Assert.assertFalse(roResult.isRuntimeToken());
    Assert.assertEquals(Long.valueOf(1L),
        metricsReportOnly.getCount(RuntimeValidationFailure.LEGACY_TOKEN));

    // Enforcing: inner run itself has no scopes, but top-level run wfRun has scopes -> 403
    RecordingMetricsCollectionService metrics = new RecordingMetricsCollectionService();
    RuntimeRequestValidator validator = createValidator(
        createSecurityConf(true, true, 60_000L), new MockProgramRunRecordFetcher(), metrics);
    HttpRequest internalReq = createInternalTokenRequest("runtime");
    RuntimeTokenRefusedException thrown = Assert.assertThrows(
        RuntimeTokenRefusedException.class,
        () -> validator.validate(sparkRun, internalReq));
    Assert.assertEquals(RuntimeValidationFailure.LEGACY_TOKEN.getReason(), thrown.getReason());
    Assert.assertEquals(Long.valueOf(1L), metrics.getCount(RuntimeValidationFailure.LEGACY_TOKEN));

    // Malformed parent workflowName in systemArgs is ignored cleanly (no 503 on parent lookup)
    ProgramRunId badParentRun = appId.spark("badParentSpark").run(RunIds.generate());
    insertLocalStartingRun(badParentRun, ImmutableMap.of(
        ProgramOptionConstants.WORKFLOW_NAME, "../other",
        ProgramOptionConstants.WORKFLOW_RUN_ID, wfRun.getRun(),
        ProgramOptionConstants.WORKFLOW_NODE_ID, "sparkNode"));
    ValidatedRuntimeRequest badParentRes = validator.validate(badParentRun,
        createInternalTokenRequest("runtime"));
    Assert.assertFalse(badParentRes.isRuntimeToken());
  }

  @Test
  public void testExtractRuntimeTokenLegacyFallbacks() throws Exception {
    ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").spark("spark")
        .run(RunIds.generate());
    insertLocalStartingRun(runId, Collections.emptyMap());

    RuntimeRequestValidator validator = createValidator(
        createSecurityConf(true, true, 60_000L), new MockProgramRunRecordFetcher(),
        new RecordingMetricsCollectionService());

    // All non-RUNTIME / undecodable headers (including empty-runtime-token and malformed
    // length-prefix payload "AgE=") must classify in memory as legacy and delegate to
    // accessEnforcer.enforce.
    String[] legacyHeaderValues = {
        null,
        Credential.CREDENTIAL_TYPE_INTERNAL + " empty-runtime-token",
        Credential.CREDENTIAL_TYPE_INTERNAL + " AgE=",
        Credential.CREDENTIAL_TYPE_INTERNAL + " YWJjZA==",
        "no-space-header",
        "Unknown-Scheme some-token",
        Credential.CREDENTIAL_TYPE_EXTERNAL_BEARER + " external-bearer-token"
    };
    for (String headerValue : legacyHeaderValues) {
      DefaultHttpRequest req = newGetRequest("/test");
      if (headerValue != null) {
        req.headers().set(Constants.Security.Headers.RUNTIME_TOKEN, headerValue);
      }
      ValidatedRuntimeRequest res = validator.validate(runId, req);
      Assert.assertFalse("Expected legacy classification for header: " + headerValue,
          res.isRuntimeToken());
    }
    Mockito.verify(accessEnforcer, Mockito.times(legacyHeaderValues.length))
        .enforce(runId, null, StandardPermission.GET);
  }

  // ---- Decision table: RUNTIME token authentication failures (401 in both modes) ----

  @Test
  public void testRuntimeTokenAuthenticationFailuresReturn401InBothModes() throws Exception {
    ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf")
        .run(RunIds.generate());
    insertLocalStartingRun(runId,
        Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson()));

    // 1. Bad signature (flipped byte in digest)
    long now = System.currentTimeMillis();
    AccessToken valid = signRuntimeToken(GSON.toJson(runId), now, now + 60_000L);
    byte[] badDigest = valid.getDigestBytes().clone();
    badDigest[0] = (byte) (badDigest[0] ^ 0xFF);
    AccessToken tampered = new AccessToken(valid.getIdentifier(), valid.getKeyId(), badDigest);

    // 2. Expired token
    AccessToken expired = signRuntimeToken(GSON.toJson(runId), now - 120_000L, now - 1_000L);

    // 3. Unknown key id
    AccessToken unknownKey = new AccessToken(valid.getIdentifier(), valid.getKeyId() + 999,
        valid.getDigestBytes());

    // 4. Identity "{}" (malformed ProgramRunId)
    AccessToken emptyJson = signRuntimeToken("{}", now, now + 60_000L);

    // 5. Identity not JSON
    AccessToken notJson = signRuntimeToken("not-a-run-id", now, now + 60_000L);

    // 6. Identity with upper-case UUID
    ProgramRunId upperRun = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf")
        .run(RunIds.generate().getId().toUpperCase());
    AccessToken upperUuid = signRuntimeToken(GSON.toJson(upperRun), now, now + 60_000L);

    // 7. Identity with version 4 (random) UUID
    ProgramRunId v4Run = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf")
        .run(UUID.randomUUID().toString());
    AccessToken v4Uuid = signRuntimeToken(GSON.toJson(v4Run), now, now + 60_000L);

    AccessToken[] invalidTokens = {
        tampered, expired, unknownKey, emptyJson, notJson, upperUuid, v4Uuid
    };

    // 8-10. Invalid signature/expiry/key-id for a well-formed run NOT in the local store;
    // validated against unseenRunId as URL run id so neither token nor URL lookup hits local store.
    ProgramRunId unseenRunId = NamespaceId.DEFAULT.app("app", "1.0").workflow("unseenWf")
        .run(RunIds.generate());
    AccessToken validUnseen = signRuntimeToken(GSON.toJson(unseenRunId), now, now + 60_000L);
    byte[] badUnseenDigest = validUnseen.getDigestBytes().clone();
    badUnseenDigest[0] = (byte) (badUnseenDigest[0] ^ 0xFF);
    AccessToken tamperedUnseen = new AccessToken(
        validUnseen.getIdentifier(), validUnseen.getKeyId(), badUnseenDigest);
    AccessToken expiredUnseen = signRuntimeToken(
        GSON.toJson(unseenRunId), now - 120_000L, now - 1_000L);
    AccessToken unknownKeyUnseen = new AccessToken(
        validUnseen.getIdentifier(), validUnseen.getKeyId() + 999, validUnseen.getDigestBytes());
    AccessToken[] unseenInvalidTokens = {tamperedUnseen, expiredUnseen, unknownKeyUnseen};

    for (boolean enforcing : new boolean[]{false, true}) {
      MockProgramRunRecordFetcher fetcher = new MockProgramRunRecordFetcher();
      RuntimeRequestValidator validator = createValidator(
          createSecurityConf(true, enforcing, 60_000L), fetcher,
          new RecordingMetricsCollectionService());
      for (AccessToken badToken : invalidTokens) {
        try {
          validator.validate(runId, createTokenRequest(badToken));
          Assert.fail("Expected UnauthenticatedException for token " + badToken);
        } catch (UnauthenticatedException expected) {
          // expected 401
        }
      }
      for (AccessToken badToken : unseenInvalidTokens) {
        try {
          validator.validate(unseenRunId, createTokenRequest(badToken));
          Assert.fail("Expected UnauthenticatedException for unseen run token " + badToken);
        } catch (UnauthenticatedException expected) {
          // expected 401
        }
      }
      Assert.assertEquals(0, fetcher.getTotalFetchCount());
    }
  }

  // ---- Decision table: RUNTIME token URL match, inner run, -SNAPSHOT, missing scopes ----

  @Test
  public void testRuntimeTokenHappyPathAndSnapshotUrlSkipsUrlLookup() throws Exception {
    for (boolean enforcing : new boolean[]{false, true}) {
      InMemoryTableService.reset();
      setupStoreAgain();
      ApplicationId versionedApp = NamespaceId.DEFAULT.app("app", "1.0");
      ProgramRunId tokenRunId = versionedApp.workflow("wf").run(RunIds.generate());
      ProgramRunId snapshotUrlRunId = NamespaceId.DEFAULT.app("app", ApplicationId.DEFAULT_VERSION)
          .workflow("wf").run(tokenRunId.getRun());

      RunRecordDetail record = createRunRecord(tokenRunId, ProgramRunStatus.RUNNING,
          ProgramRunClusterStatus.PROVISIONED,
          Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson()));
      MockProgramRunRecordFetcher fetcher = new MockProgramRunRecordFetcher().putRunRecord(record);
      RecordingMetricsCollectionService metrics = new RecordingMetricsCollectionService();
      RuntimeRequestValidator validator = createValidator(
          createSecurityConf(true, enforcing, 60_000L), fetcher, metrics);

      ValidatedRuntimeRequest validated = validator.validate(snapshotUrlRunId,
          createRuntimeTokenRequest(tokenRunId));
      Assert.assertTrue(validated.isRuntimeToken());
      Assert.assertEquals(tokenRunId, validated.getTokenRunId());
      Assert.assertEquals(SAMPLE_SCOPES, validated.getRuntimeScopes());
      Assert.assertEquals(ProgramRunStatus.RUNNING,
          validated.getProgramRunInfo().getProgramRunStatus());

      // Only tokenRunId was fetched; snapshotUrlRunId was NEVER looked up or written locally
      Assert.assertEquals(1, fetcher.getFetchCount(tokenRunId));
      Assert.assertEquals(0, fetcher.getFetchCount(snapshotUrlRunId));
      RunRecordDetail localSnapshotRecord = TransactionRunners.run(txRunner, context -> {
        return AppMetadataStore.create(context).getRun(snapshotUrlRunId);
      });
      Assert.assertNull(localSnapshotRecord);
      Assert.assertTrue(metrics.getCounts().isEmpty());
    }
  }

  @Test
  public void testRuntimeTokenInnerSparkRunAllowed() throws Exception {
    ApplicationId appId = NamespaceId.DEFAULT.app("app", "1.0");
    ProgramRunId wfRun = appId.workflow("wf").run(RunIds.generate());
    ProgramRunId sparkRun = appId.spark("innerSpark").run(RunIds.generate());

    insertLocalStartingRun(wfRun,
        Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson()));
    insertLocalStartingRun(sparkRun, ImmutableMap.of(
        ProgramOptionConstants.WORKFLOW_NAME, wfRun.getProgram(),
        ProgramOptionConstants.WORKFLOW_RUN_ID, wfRun.getRun(),
        ProgramOptionConstants.WORKFLOW_NODE_ID, "sparkNode"));

    for (boolean enforcing : new boolean[]{false, true}) {
      RecordingMetricsCollectionService metrics = new RecordingMetricsCollectionService();
      RuntimeRequestValidator validator = createValidator(
          createSecurityConf(true, enforcing, 60_000L), new MockProgramRunRecordFetcher(), metrics);

      ValidatedRuntimeRequest validated = validator.validate(sparkRun,
          createRuntimeTokenRequest(wfRun));
      Assert.assertTrue(validated.isRuntimeToken());
      Assert.assertEquals(wfRun, validated.getTokenRunId());
      Assert.assertEquals(SAMPLE_SCOPES, validated.getRuntimeScopes());
      Assert.assertTrue(metrics.getCounts().isEmpty());
    }
  }

  @Test
  public void testRuntimeTokenWithMissingAndFinishedTokenRunBothModes() throws Exception {
    ApplicationId appId = NamespaceId.DEFAULT.app("app", "1.0");
    ProgramRunId missingTokenRun = appId.workflow("missingTokenRun").run(RunIds.generate());
    ProgramRunId localFinishedTokenRun = appId.workflow("localFinTokenRun").run(RunIds.generate());
    ProgramRunId fetchedFinishedTokenRun = appId.workflow("fetchedFinTokenRun")
        .run(RunIds.generate());

    insertLocalCompletedRun(localFinishedTokenRun,
        Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson()),
        ProgramRunStatus.COMPLETED);

    for (boolean enforcing : new boolean[]{false, true}) {
      MockProgramRunRecordFetcher fetcher = new MockProgramRunRecordFetcher()
          .putRunRecord(createRunRecord(fetchedFinishedTokenRun, ProgramRunStatus.FAILED,
              ProgramRunClusterStatus.DEPROVISIONED,
              Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES,
                  SAMPLE_SCOPES.toJson())));
      RuntimeRequestValidator validator = createValidator(
          createSecurityConf(true, enforcing, 60_000L), fetcher,
          new RecordingMetricsCollectionService());

      // Token run missing -> 400 and cached
      for (int i = 0; i < 2; i++) {
        try {
          validator.validate(missingTokenRun, createRuntimeTokenRequest(missingTokenRun));
          Assert.fail("Expected BadRequestException for missing token run");
        } catch (BadRequestException expected) {
          // expected 400
        }
      }
      Assert.assertEquals(1, fetcher.getFetchCount(missingTokenRun));

      // Token run finished locally -> 410
      try {
        validator.validate(localFinishedTokenRun,
            createRuntimeTokenRequest(localFinishedTokenRun));
        Assert.fail("Expected GoneException for locally finished token run");
      } catch (GoneException expected) {
        // expected 410
      }

      // Token run finished from fetch -> 400, not written locally, cached
      for (int i = 0; i < 2; i++) {
        try {
          validator.validate(fetchedFinishedTokenRun,
              createRuntimeTokenRequest(fetchedFinishedTokenRun));
          Assert.fail("Expected BadRequestException for fetched finished token run");
        } catch (BadRequestException expected) {
          // expected 400
        }
      }
      Assert.assertEquals(1, fetcher.getFetchCount(fetchedFinishedTokenRun));
      Assert.assertNull(TransactionRunners.run(txRunner, context -> {
        return AppMetadataStore.create(context).getRun(fetchedFinishedTokenRun);
      }));
    }
  }

  @Test
  public void testRuntimeTokenUrlMismatchAndMissingScopesBothModes() throws Exception {
    ApplicationId appId = NamespaceId.DEFAULT.app("app", "1.0");
    ProgramRunId tokenRun = appId.workflow("wf1").run(RunIds.generate());
    ProgramRunId otherRun = appId.spark("unrelated").run(RunIds.generate());

    // Case 1: URL mismatch (tokenRun has scopes, otherRun is unrelated)
    insertLocalStartingRun(tokenRun,
        Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson()));
    insertLocalStartingRun(otherRun, Collections.emptyMap());

    // Report-only: counts url_mismatch and continues
    RecordingMetricsCollectionService metricsReportOnly = new RecordingMetricsCollectionService();
    RuntimeRequestValidator validatorReportOnly = createValidator(
        createSecurityConf(true, false, 60_000L), new MockProgramRunRecordFetcher(),
        metricsReportOnly);
    ValidatedRuntimeRequest roRes = validatorReportOnly.validate(otherRun,
        createRuntimeTokenRequest(tokenRun));
    Assert.assertTrue(roRes.isRuntimeToken());
    Assert.assertEquals(tokenRun, roRes.getTokenRunId());
    Assert.assertEquals(Long.valueOf(1L),
        metricsReportOnly.getCount(RuntimeValidationFailure.URL_MISMATCH));

    // Enforcing: counts url_mismatch and throws 403
    RecordingMetricsCollectionService metricsEnf = new RecordingMetricsCollectionService();
    RuntimeRequestValidator validatorEnf = createValidator(
        createSecurityConf(true, true, 60_000L), new MockProgramRunRecordFetcher(), metricsEnf);
    HttpRequest mismatchReq = createRuntimeTokenRequest(tokenRun);
    RuntimeTokenRefusedException mismatchThrown = Assert.assertThrows(
        RuntimeTokenRefusedException.class,
        () -> validatorEnf.validate(otherRun, mismatchReq));
    Assert.assertEquals(
        RuntimeValidationFailure.URL_MISMATCH.getReason(), mismatchThrown.getReason());
    Assert.assertEquals(Long.valueOf(1L),
        metricsEnf.getCount(RuntimeValidationFailure.URL_MISMATCH));

    // Case 2: Missing scopes on token run (both absent and malformed JSON)
    ProgramRunId noScopesRun = appId.spark("noScopes").run(RunIds.generate());
    insertLocalStartingRun(noScopesRun, Collections.emptyMap());
    ProgramRunId badScopesRun = appId.spark("badScopes").run(RunIds.generate());
    insertLocalStartingRun(badScopesRun,
        Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, "{not-valid-json"));

    for (ProgramRunId runWithoutValidScopes : new ProgramRunId[]{noScopesRun, badScopesRun}) {
      RecordingMetricsCollectionService metricsRo = new RecordingMetricsCollectionService();
      RuntimeRequestValidator validatorRo = createValidator(
          createSecurityConf(true, false, 60_000L), new MockProgramRunRecordFetcher(), metricsRo);
      ValidatedRuntimeRequest res = validatorRo.validate(runWithoutValidScopes,
          createRuntimeTokenRequest(runWithoutValidScopes));
      Assert.assertTrue(res.isRuntimeToken());
      Assert.assertNull(res.getRuntimeScopes());
      Assert.assertEquals(Long.valueOf(1L),
          metricsRo.getCount(RuntimeValidationFailure.MISSING_SCOPES));

      RecordingMetricsCollectionService mEnf = new RecordingMetricsCollectionService();
      RuntimeRequestValidator vEnf = createValidator(
          createSecurityConf(true, true, 60_000L), new MockProgramRunRecordFetcher(), mEnf);
      HttpRequest missingScopesReq = createRuntimeTokenRequest(runWithoutValidScopes);
      RuntimeTokenRefusedException missingScopesThrown = Assert.assertThrows(
          RuntimeTokenRefusedException.class,
          () -> vEnf.validate(runWithoutValidScopes, missingScopesReq));
      Assert.assertEquals(
          RuntimeValidationFailure.MISSING_SCOPES.getReason(), missingScopesThrown.getReason());
      Assert.assertEquals(Long.valueOf(1L),
          mEnf.getCount(RuntimeValidationFailure.MISSING_SCOPES));
    }
  }

  @Test
  public void testRuntimeTokenStructurallyIneligibleUrlSkipsLookupInEnforcingMode()
      throws Exception {
    ApplicationId appV1 = NamespaceId.DEFAULT.app("app", "1.0");
    ProgramRunId wfTokenRun = appV1.workflow("wf").run(RunIds.generate());
    ProgramRunId sparkTokenRun = appV1.spark("standaloneSpark").run(RunIds.generate());
    insertLocalStartingRun(wfTokenRun,
        Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson()));
    insertLocalStartingRun(sparkTokenRun,
        Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson()));

    // Structurally ineligible URL targets seeded as active in the remote fetcher.
    ProgramRunId crossNsRun = new NamespaceId("otherNs").app("app", "1.0")
        .spark("s").run(RunIds.generate());
    ProgramRunId crossAppRun = NamespaceId.DEFAULT.app("otherApp", "1.0")
        .spark("s").run(RunIds.generate());
    ProgramRunId sameAppOtherRun = appV1.spark("otherSpark").run(RunIds.generate());
    ProgramRunId diffVersionRun = NamespaceId.DEFAULT.app("app", "2.0")
        .spark("s").run(RunIds.generate());
    ProgramRunId siblingWfRun = appV1.workflow("wf").run(RunIds.generate());
    ProgramRunId sameAppServiceRun = appV1.service("svc").run(RunIds.generate());

    MockProgramRunRecordFetcher enforcingFetcher = new MockProgramRunRecordFetcher();
    for (ProgramRunId seeded : ImmutableList.of(
        crossNsRun, crossAppRun, sameAppOtherRun, diffVersionRun,
        siblingWfRun, sameAppServiceRun)) {
      enforcingFetcher.putRunRecord(createRunRecord(seeded, ProgramRunStatus.RUNNING,
          ProgramRunClusterStatus.PROVISIONED, Collections.emptyMap()));
    }

    // Enforcing mode: zero remote lookups and zero local store inserts for every ineligible URL
    // (including a nonexistent run, which uniformly returns 403 rather than 400).
    ProgramRunId nonexistentCrossAppRun = NamespaceId.DEFAULT.app("missingApp", "1.0")
        .spark("s").run(RunIds.generate());
    for (ProgramRunId wfIneligibleUrl : ImmutableList.of(
        crossNsRun, crossAppRun, diffVersionRun, siblingWfRun,
        sameAppServiceRun, nonexistentCrossAppRun)) {
      assertIneligibleUrlSkipsLookup(enforcingFetcher, wfTokenRun, wfIneligibleUrl);
    }
    assertIneligibleUrlSkipsLookup(enforcingFetcher, sparkTokenRun, sameAppOtherRun);
    Assert.assertEquals(0, enforcingFetcher.getTotalFetchCount());

    // Inner MAPREDUCE run in the same app+version is still allowed for a WORKFLOW token.
    ProgramRunId innerMrRun = appV1.mr("innerMr").run(RunIds.generate());
    insertLocalStartingRun(innerMrRun, ImmutableMap.of(
        ProgramOptionConstants.WORKFLOW_NAME, wfTokenRun.getProgram(),
        ProgramOptionConstants.WORKFLOW_RUN_ID, wfTokenRun.getRun(),
        ProgramOptionConstants.WORKFLOW_NODE_ID, "mrNode"));
    RecordingMetricsCollectionService mrMetrics = new RecordingMetricsCollectionService();
    RuntimeRequestValidator mrValidator = createValidator(
        createSecurityConf(true, true, 60_000L), enforcingFetcher, mrMetrics);
    ValidatedRuntimeRequest mrValidated = mrValidator.validate(innerMrRun,
        createRuntimeTokenRequest(wfTokenRun));
    Assert.assertTrue(mrValidated.isRuntimeToken());
    Assert.assertEquals(wfTokenRun, mrValidated.getTokenRunId());
    Assert.assertTrue(mrMetrics.getCounts().isEmpty());

    // Report-only mode: active cross-app URL still succeeds and URL_MISMATCH is counted once.
    MockProgramRunRecordFetcher roFetcher = new MockProgramRunRecordFetcher()
        .putRunRecord(createRunRecord(crossAppRun, ProgramRunStatus.RUNNING,
            ProgramRunClusterStatus.PROVISIONED, Collections.emptyMap()));
    RecordingMetricsCollectionService roMetrics = new RecordingMetricsCollectionService();
    RuntimeRequestValidator roValidator = createValidator(
        createSecurityConf(true, false, 60_000L), roFetcher, roMetrics);
    ValidatedRuntimeRequest roValidated = roValidator.validate(crossAppRun,
        createRuntimeTokenRequest(wfTokenRun));
    Assert.assertTrue(roValidated.isRuntimeToken());
    Assert.assertEquals(wfTokenRun, roValidated.getTokenRunId());
    Assert.assertEquals(ProgramRunStatus.RUNNING,
        roValidated.getProgramRunInfo().getProgramRunStatus());
    Assert.assertEquals(Long.valueOf(1L),
        roMetrics.getCount(RuntimeValidationFailure.URL_MISMATCH));
  }

  private void assertIneligibleUrlSkipsLookup(MockProgramRunRecordFetcher fetcher,
      ProgramRunId tokenRun, ProgramRunId urlRun) throws Exception {
    RecordingMetricsCollectionService metrics = new RecordingMetricsCollectionService();
    RuntimeRequestValidator validator = createValidator(
        createSecurityConf(true, true, 60_000L), fetcher, metrics);
    HttpRequest req = createRuntimeTokenRequest(tokenRun);
    String contextMsg = "URL " + urlRun + " for token " + tokenRun;
    RuntimeTokenRefusedException thrown = Assert.assertThrows(
        contextMsg, RuntimeTokenRefusedException.class, () -> validator.validate(urlRun, req));
    Assert.assertEquals(contextMsg, RuntimeValidationFailure.URL_MISMATCH.getReason(),
        thrown.getReason());
    Assert.assertEquals(contextMsg, Long.valueOf(1L),
        metrics.getCount(RuntimeValidationFailure.URL_MISMATCH));
    Assert.assertEquals(contextMsg, 0, fetcher.getFetchCount(urlRun));
    RunRecordDetail localRow = TransactionRunners.run(txRunner, context -> {
      return AppMetadataStore.create(context).getRun(urlRun);
    });
    Assert.assertNull("Expected no local row for " + contextMsg, localRow);
  }

  @Test
  public void testRuntimeTokenUrlRunStateCheckedWhenDifferentFromTokenRun() throws Exception {
    ApplicationId appId = NamespaceId.DEFAULT.app("app", "1.0");
    ProgramRunId wfRun = appId.workflow("wf").run(RunIds.generate());
    ProgramRunId missingInner = appId.spark("missing").run(RunIds.generate());
    ProgramRunId finishedInner = appId.spark("finished").run(RunIds.generate());

    insertLocalStartingRun(wfRun,
        Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson()));
    insertLocalCompletedRun(finishedInner, ImmutableMap.of(
        ProgramOptionConstants.WORKFLOW_NAME, wfRun.getProgram(),
        ProgramOptionConstants.WORKFLOW_RUN_ID, wfRun.getRun(),
        ProgramOptionConstants.WORKFLOW_NODE_ID, "sparkNode"), ProgramRunStatus.COMPLETED);

    for (boolean enforcing : new boolean[]{false, true}) {
      RuntimeRequestValidator validator = createValidator(
          createSecurityConf(true, enforcing, 60_000L), new MockProgramRunRecordFetcher(),
          new RecordingMetricsCollectionService());

      try {
        validator.validate(missingInner, createRuntimeTokenRequest(wfRun));
        Assert.fail("Expected BadRequestException for missing URL run");
      } catch (BadRequestException expected) {
        // expected 400
      }

      try {
        validator.validate(finishedInner, createRuntimeTokenRequest(wfRun));
        Assert.fail("Expected GoneException for locally finished URL run");
      } catch (GoneException expected) {
        // expected 410
      }
    }
  }

  // ---- Lookup table & Cache tests (positive POLL_TIME_MS = 60s) ----

  @Test
  public void testMalformedUrlRunIdReturns400WithoutLookup() throws Exception {
    ProgramRunId validTokenRun = NamespaceId.DEFAULT.app("app", "1.0").spark("s")
        .run(RunIds.generate());
    insertLocalStartingRun(validTokenRun,
        Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson()));

    ProgramRunId upperRun = NamespaceId.DEFAULT.app("app", "1.0").spark("s")
        .run(RunIds.generate().getId().toUpperCase());
    ProgramRunId v4Run = NamespaceId.DEFAULT.app("app", "1.0").spark("s")
        .run(UUID.randomUUID().toString());
    ProgramRunId nonUuid = NamespaceId.DEFAULT.app("app", "1.0").spark("s").run("not-a-uuid");
    ProgramRunId badVersionSpace = NamespaceId.DEFAULT.app("app", "bad ver").spark("s")
        .run(validTokenRun.getRun());
    ProgramRunId badVersionDotDot = NamespaceId.DEFAULT.app("app", "..").spark("s")
        .run(validTokenRun.getRun());
    ProgramRunId badProgramTraversal = NamespaceId.DEFAULT.app("app", "1.0").spark("../other")
        .run(RunIds.generate());
    ProgramRunId[] malformedIds = {
        upperRun, v4Run, nonUuid, badVersionSpace, badVersionDotDot, badProgramTraversal
    };

    for (boolean enforcing : new boolean[]{false, true}) {
      MockProgramRunRecordFetcher fetcher = new MockProgramRunRecordFetcher();
      RuntimeRequestValidator validator = createValidator(
          createSecurityConf(true, enforcing, 60_000L), fetcher,
          new RecordingMetricsCollectionService());

      for (ProgramRunId bad : malformedIds) {
        try {
          validator.validate(bad, createInternalTokenRequest("runtime"));
          Assert.fail("Expected BadRequestException (legacy) for " + bad);
        } catch (BadRequestException expected) {
          // expected 400
        }
        try {
          validator.validate(bad, createRuntimeTokenRequest(validTokenRun));
          Assert.fail("Expected BadRequestException (RUNTIME) for " + bad);
        } catch (BadRequestException expected) {
          // expected 400
        }
      }
      Assert.assertEquals(0, fetcher.getTotalFetchCount());
    }
  }

  @Test
  public void testNoRecordIsCachedForPollTime() throws Exception {
    for (boolean enforcing : new boolean[]{false, true}) {
      ProgramRunId missingRun = NamespaceId.DEFAULT.app("app", "1.0").spark("s")
          .run(RunIds.generate());
      MockProgramRunRecordFetcher fetcher = new MockProgramRunRecordFetcher();
      RuntimeRequestValidator validator = createValidator(
          createSecurityConf(true, enforcing, 60_000L), fetcher,
          new RecordingMetricsCollectionService());

      for (int i = 0; i < 2; i++) {
        try {
          validator.validate(missingRun, createInternalTokenRequest("runtime"));
          Assert.fail("Expected BadRequestException");
        } catch (BadRequestException expected) {
          // expected 400
        }
      }
      // Cached on first miss; second request did not call fetcher again
      Assert.assertEquals(1, fetcher.getFetchCount(missingRun));
    }
  }

  @Test
  public void testNotLaunchedYetLocalAndFetchedNotStoredNotCached() throws Exception {
    for (boolean enforcing : new boolean[]{false, true}) {
      InMemoryTableService.reset();
      setupStoreAgain();
      CConfiguration conf = createSecurityConf(true, enforcing, 60_000L);

      // 1. Local PENDING -> 400, not cached; once launched, succeeds immediately
      ProgramRunId localPending = NamespaceId.DEFAULT.app("app", "1.0").spark("localPending")
          .run(RunIds.generate());
      TransactionRunners.run(txRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        store.recordProgramProvisioning(localPending, Collections.emptyMap(),
            ImmutableMap.of(
                SystemArguments.PROFILE_NAME, "system:default",
                ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson()),
            createSourceId(1), ARTIFACT_ID);
      });
      MockProgramRunRecordFetcher fetcher = new MockProgramRunRecordFetcher();
      RuntimeRequestValidator validator = createValidator(conf, fetcher,
          new RecordingMetricsCollectionService());

      try {
        validator.validate(localPending, createRuntimeTokenRequest(localPending));
        Assert.fail("Expected BadRequestException for local PENDING");
      } catch (BadRequestException expected) {
        // expected 400
      }
      // Launch the run; next request must succeed immediately despite 60s poll time
      TransactionRunners.run(txRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        store.recordProgramProvisioned(localPending, 1, createSourceId(2));
        store.recordProgramStart(localPending, null, Collections.emptyMap(), createSourceId(3));
      });
      ValidatedRuntimeRequest afterLocalLaunch = validator.validate(localPending,
          createRuntimeTokenRequest(localPending));
      Assert.assertEquals(ProgramRunStatus.STARTING,
          afterLocalLaunch.getProgramRunInfo().getProgramRunStatus());

      // 2. Local STOPPING with cluster PROVISIONING -> 400, not cached
      ProgramRunId localStoppingProv = NamespaceId.DEFAULT.app("app", "1.0")
          .spark("localStoppingProv").run(RunIds.generate());
      long nowSec = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
      TransactionRunners.run(txRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        store.recordProgramProvisioning(localStoppingProv, Collections.emptyMap(),
            ImmutableMap.of(
                SystemArguments.PROFILE_NAME, "system:default",
                ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson()),
            createSourceId(10), ARTIFACT_ID);
        store.recordProgramStopping(localStoppingProv, createSourceId(11), nowSec, nowSec + 300);
      });
      try {
        validator.validate(localStoppingProv, createRuntimeTokenRequest(localStoppingProv));
        Assert.fail("Expected BadRequestException for local STOPPING+PROVISIONING");
      } catch (BadRequestException expected) {
        // expected 400
      }
      TransactionRunners.run(txRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        store.recordProgramProvisioned(localStoppingProv, 1, createSourceId(12));
      });
      ValidatedRuntimeRequest afterProv = validator.validate(localStoppingProv,
          createRuntimeTokenRequest(localStoppingProv));
      Assert.assertEquals(ProgramRunStatus.STOPPING,
          afterProv.getProgramRunInfo().getProgramRunStatus());

      // 3. Fetched PENDING & fetched STOPPING+PROVISIONING -> 400, NOT stored locally, NOT cached
      ProgramRunId fetchedPending = NamespaceId.DEFAULT.app("app", "1.0").spark("fetchedPending")
          .run(RunIds.generate());
      fetcher.putRunRecord(createRunRecord(fetchedPending, ProgramRunStatus.PENDING,
          ProgramRunClusterStatus.PROVISIONING,
          Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson())));
      try {
        validator.validate(fetchedPending, createRuntimeTokenRequest(fetchedPending));
        Assert.fail("Expected BadRequestException for fetched PENDING");
      } catch (BadRequestException expected) {
        // expected 400
      }
      Assert.assertNull(TransactionRunners.run(txRunner, context -> {
        return AppMetadataStore.create(context).getRun(fetchedPending);
      }));

      // Update fetcher to STOPPING+PROVISIONING: still 400, not stored, not cached
      fetcher.putRunRecord(createRunRecord(fetchedPending, ProgramRunStatus.STOPPING,
          ProgramRunClusterStatus.PROVISIONING,
          Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson())));
      try {
        validator.validate(fetchedPending, createRuntimeTokenRequest(fetchedPending));
        Assert.fail("Expected BadRequestException for fetched STOPPING+PROVISIONING");
      } catch (BadRequestException expected) {
        // expected 400
      }
      Assert.assertEquals(2, fetcher.getFetchCount(fetchedPending));
      Assert.assertNull(TransactionRunners.run(txRunner, context -> {
        return AppMetadataStore.create(context).getRun(fetchedPending);
      }));

      // Now launched (STARTING + PROVISIONED) -> succeeds immediately and is stored locally
      fetcher.putRunRecord(createRunRecord(fetchedPending, ProgramRunStatus.STARTING,
          ProgramRunClusterStatus.PROVISIONED,
          Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson())));
      ValidatedRuntimeRequest afterFetchedLaunch = validator.validate(fetchedPending,
          createRuntimeTokenRequest(fetchedPending));
      Assert.assertEquals(ProgramRunStatus.STARTING,
          afterFetchedLaunch.getProgramRunInfo().getProgramRunStatus());
      Assert.assertEquals(3, fetcher.getFetchCount(fetchedPending));
      RunRecordDetail storedAfterLaunch = TransactionRunners.run(txRunner, context -> {
        return AppMetadataStore.create(context).getRun(fetchedPending);
      });
      Assert.assertNotNull(storedAfterLaunch);
      Assert.assertEquals(ProgramRunStatus.STARTING, storedAfterLaunch.getStatus());
    }
  }

  @Test
  public void testFinishedFetchedAndNeverProvisionedRunsCachedAndNotWritten() throws Exception {
    for (boolean enforcing : new boolean[]{false, true}) {
      InMemoryTableService.reset();
      setupStoreAgain();
      CConfiguration conf = createSecurityConf(true, enforcing, 60_000L);
      MockProgramRunRecordFetcher fetcher = new MockProgramRunRecordFetcher();
      RuntimeRequestValidator validator = createValidator(conf, fetcher,
          new RecordingMetricsCollectionService());

      // 1. Fetched COMPLETED -> 400, not stored, cached
      ProgramRunId fetchedCompleted = NamespaceId.DEFAULT.app("app", "1.0").spark("fComp")
          .run(RunIds.generate());
      fetcher.putRunRecord(createRunRecord(fetchedCompleted, ProgramRunStatus.COMPLETED,
          ProgramRunClusterStatus.DEPROVISIONED, Collections.emptyMap()));
      for (int i = 0; i < 2; i++) {
        try {
          validator.validate(fetchedCompleted, createInternalTokenRequest("runtime"));
          Assert.fail("Expected BadRequestException");
        } catch (BadRequestException expected) {
          // expected 400
        }
      }
      Assert.assertEquals(1, fetcher.getFetchCount(fetchedCompleted));
      Assert.assertNull(TransactionRunners.run(txRunner, context -> {
        return AppMetadataStore.create(context).getRun(fetchedCompleted);
      }));

      // 2. Fetched REJECTED (never provisioned) -> 400, not stored, cached
      ProgramRunId fetchedRejected = NamespaceId.DEFAULT.app("app", "1.0").spark("fRej")
          .run(RunIds.generate());
      fetcher.putRunRecord(createRunRecord(fetchedRejected, ProgramRunStatus.REJECTED,
          null, Collections.emptyMap()));
      for (int i = 0; i < 2; i++) {
        try {
          validator.validate(fetchedRejected, createInternalTokenRequest("runtime"));
          Assert.fail("Expected BadRequestException");
        } catch (BadRequestException expected) {
          // expected 400
        }
      }
      Assert.assertEquals(1, fetcher.getFetchCount(fetchedRejected));
      Assert.assertNull(TransactionRunners.run(txRunner, context -> {
        return AppMetadataStore.create(context).getRun(fetchedRejected);
      }));

      // 3. Fetched KILLED while cluster still PROVISIONING -> finished decided first -> 400, cached
      ProgramRunId fetchedKilledPending = NamespaceId.DEFAULT.app("app", "1.0").spark("fKillPend")
          .run(RunIds.generate());
      fetcher.putRunRecord(createRunRecord(fetchedKilledPending, ProgramRunStatus.KILLED,
          ProgramRunClusterStatus.PROVISIONING, Collections.emptyMap()));
      for (int i = 0; i < 2; i++) {
        try {
          validator.validate(fetchedKilledPending, createInternalTokenRequest("runtime"));
          Assert.fail("Expected BadRequestException");
        } catch (BadRequestException expected) {
          // expected 400
        }
      }
      Assert.assertEquals(1, fetcher.getFetchCount(fetchedKilledPending));

      // 4. Local REJECTED and local KILLED while pending -> 410 (GoneException), cached
      ProgramRunId localRejected = NamespaceId.DEFAULT.app("app", "1.0").spark("lRej")
          .run(RunIds.generate());
      TransactionRunners.run(txRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        store.recordProgramRejected(localRejected, Collections.emptyMap(), Collections.emptyMap(),
            createSourceId(20), null);
      });
      ProgramRunId localKilledPending = NamespaceId.DEFAULT.app("app", "1.0").spark("lKillPend")
          .run(RunIds.generate());
      TransactionRunners.run(txRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        store.recordProgramProvisioning(localKilledPending, Collections.emptyMap(),
            Collections.singletonMap(SystemArguments.PROFILE_NAME, "system:default"),
            createSourceId(21), ARTIFACT_ID);
        store.recordProgramStop(localKilledPending, System.currentTimeMillis(),
            ProgramRunStatus.KILLED, null, createSourceId(22));
      });

      // Trigger first 410 for both local ended runs, then clear local history to prove the
      // subsequent 410s are served from the cache (not re-read from the store).
      for (ProgramRunId localEndRun : new ProgramRunId[]{localRejected, localKilledPending}) {
        try {
          validator.validate(localEndRun, createInternalTokenRequest("runtime"));
          Assert.fail("Expected GoneException for " + localEndRun);
        } catch (GoneException expected) {
          // expected 410
        }
      }
      TransactionRunners.run(txRunner, context -> {
        AppMetadataStore.create(context).deleteProgramHistory(NamespaceId.DEFAULT);
      });
      for (ProgramRunId localEndRun : new ProgramRunId[]{localRejected, localKilledPending}) {
        Assert.assertNull(TransactionRunners.run(txRunner, context -> {
          return AppMetadataStore.create(context).getRun(localEndRun);
        }));
        try {
          validator.validate(localEndRun, createInternalTokenRequest("runtime"));
          Assert.fail("Expected cached GoneException for " + localEndRun);
        } catch (GoneException expected) {
          // served from cache
        }
        Assert.assertEquals(0, fetcher.getFetchCount(localEndRun));
      }
    }
  }

  // ---- Concurrent cold requests (legacy + RUNTIME) ----

  @Test
  public void testConcurrentColdRequestsLegacyAndRuntimeBothSucceedWithSingleFetch()
      throws Exception {
    CConfiguration conf = createSecurityConf(true, false, 60_000L);
    ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf")
        .run(RunIds.generate());
    RunRecordDetail record = createRunRecord(runId, ProgramRunStatus.RUNNING,
        ProgramRunClusterStatus.PROVISIONED,
        Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson()));

    CountDownLatch fetchEntered = new CountDownLatch(1);
    CountDownLatch releaseFetch = new CountDownLatch(1);
    MockProgramRunRecordFetcher fetcher = new MockProgramRunRecordFetcher()
        .putRunRecord(record)
        .withFetchBarrier(fetchEntered, releaseFetch);

    RecordingMetricsCollectionService metrics = new RecordingMetricsCollectionService();
    RuntimeRequestValidator validator = createValidator(conf, fetcher, metrics);
    AtomicReference<Thread> legacyThreadRef = new AtomicReference<>();
    CountDownLatch legacyEnforceEntered = new CountDownLatch(1);
    Mockito.doAnswer(invocation -> {
      legacyThreadRef.set(Thread.currentThread());
      legacyEnforceEntered.countDown();
      return null;
    }).when(accessEnforcer).enforce(runId, null, StandardPermission.GET);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      final Future<ValidatedRuntimeRequest> runtimeFuture = pool.submit(
          () -> validator.validate(runId, createRuntimeTokenRequest(runId)));
      Assert.assertTrue(fetchEntered.await(5, TimeUnit.SECONDS));
      Future<ValidatedRuntimeRequest> legacyFuture = pool.submit(
          () -> validator.validate(runId, createInternalTokenRequest("runtime")));
      awaitThreadBlocked(legacyEnforceEntered, legacyThreadRef);
      releaseFetch.countDown();

      ValidatedRuntimeRequest runtimeRes = runtimeFuture.get(5, TimeUnit.SECONDS);
      ValidatedRuntimeRequest legacyRes = legacyFuture.get(5, TimeUnit.SECONDS);

      Assert.assertTrue(runtimeRes.isRuntimeToken());
      Assert.assertEquals(SAMPLE_SCOPES, runtimeRes.getRuntimeScopes());
      Assert.assertFalse(legacyRes.isRuntimeToken());
      Assert.assertEquals(ProgramRunStatus.RUNNING,
          legacyRes.getProgramRunInfo().getProgramRunStatus());
      Assert.assertEquals(1, fetcher.getFetchCount(runId));
      Assert.assertEquals(Long.valueOf(1L),
          metrics.getCount(RuntimeValidationFailure.LEGACY_TOKEN));
    } finally {
      pool.shutdownNow();
    }
  }

  private static void awaitThreadBlocked(CountDownLatch entered,
      AtomicReference<Thread> threadRef) throws InterruptedException {
    Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    Thread thread = threadRef.get();
    while (thread.getState() != Thread.State.WAITING
        && thread.getState() != Thread.State.BLOCKED) {
      if (System.nanoTime() >= deadlineNanos) {
        Assert.fail("Timed out waiting for legacy thread to block on cache load; state="
            + thread.getState());
      }
      Thread.yield();
    }
  }

  // ---- Real HTTP RemoteProgramRunRecordFetcher against InternalAccessEnforcer ----

  @Test
  public void testRemoteFetchUsesSystemTokenAcceptedByInternalAccessEnforcer() throws Exception {
    CConfiguration secConf = createSecurityConf(true, true, 60_000L);
    Injector secInjector = Guice.createInjector(
        new ConfigModule(secConf),
        new IOModule(),
        new InMemoryDiscoveryModule(),
        new CoreSecurityRuntimeModule().getInMemoryModules(),
        new AuthenticationContextModules().getMasterModule());

    TokenManager secTokenManager = secInjector.getInstance(TokenManager.class);
    Services.startAndWait(secTokenManager);
    try {
      Codec<AccessToken> secCodec = secInjector.getInstance(
          Key.get(new TypeLiteral<Codec<AccessToken>>() { }));
      InternalAccessEnforcer internalAccessEnforcer =
          secInjector.getInstance(InternalAccessEnforcer.class);
      AuthenticationContext systemAuthContext =
          secInjector.getInstance(AuthenticationContext.class);

      ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf")
          .run(RunIds.generate());
      RunRecordDetail runRecord = createRunRecord(runId, ProgramRunStatus.RUNNING,
          ProgramRunClusterStatus.PROVISIONED,
          Collections.singletonMap(ProgramOptionConstants.RUNTIME_SCOPES, SAMPLE_SCOPES.toJson()));

      long now = System.currentTimeMillis();
      AccessToken runtimeAccessToken = secTokenManager.signIdentifier(new UserIdentity(
          GSON.toJson(runId), UserIdentity.IdentifierType.RUNTIME,
          Collections.emptyList(), now, now + 60_000L));
      String runtimeTokenStr = Base64.getEncoder().encodeToString(
          secCodec.encode(runtimeAccessToken));

      AtomicReference<String> receivedTokenHeader = new AtomicReference<>();
      NettyHttpService appFabricService = NettyHttpService.builder(Constants.Service.APP_FABRIC_HTTP)
          .setHost(InetAddress.getLocalHost().getCanonicalHostName())
          .setExceptionHandler(new HttpExceptionHandler())
          .setChannelPipelineModifier(new ChannelPipelineModifier() {
            @Override
            public void modify(ChannelPipeline pipeline) {
              // Use dispatcher's EventExecutor so AuthenticationChannelHandler and the HTTP
              // handler share the same thread for SecurityRequestContext (ThreadLocal).
              EventExecutor executor = pipeline.context("dispatcher").executor();
              pipeline.addBefore(executor, "dispatcher", "authenticator",
                  new AuthenticationChannelHandler(true, false, false, null, null));
            }
          })
          .setHttpHandlers(new EnforcedRunRecordHandler(
              receivedTokenHeader, internalAccessEnforcer, systemAuthContext, runRecord))
          .build();
      appFabricService.start();
      Cancellable registration = secInjector.getInstance(DiscoveryService.class)
          .register(URIScheme.createDiscoverable(Constants.Service.APP_FABRIC_HTTP,
              appFabricService));

      try {
        RemoteClientFactory remoteClientFactory = new RemoteClientFactory(
            secInjector.getInstance(DiscoveryServiceClient.class),
            new DefaultInternalAuthenticator(systemAuthContext),
            new NoOpRemoteAuthenticator());
        ProgramRunRecordFetcher realFetcher = new RemoteProgramRunRecordFetcher(remoteClientFactory);

        RuntimeValidationReporter reporter = new RuntimeValidationReporter(
            secConf, new NoOpMetricsCollectionService());
        final DirectRuntimeRequestValidator validator = new DirectRuntimeRequestValidator(
            secConf, txRunner, realFetcher, accessEnforcer, authenticationContext,
            secTokenManager, secCodec, reporter);

        // Simulate caller's SecurityRequestContext populated with the RUNTIME token
        Credential callerCred = new Credential(runtimeTokenStr, Credential.CredentialType.INTERNAL);
        SecurityRequestContext.reset();
        SecurityRequestContext.setUserId("runtime");
        SecurityRequestContext.setUserIp("10.0.0.9");
        SecurityRequestContext.setUserCredential(callerCred);

        // Prove over the wire that without callAsSystem the handler's InternalAccessEnforcer
        // rejects the caller's RUNTIME token.
        try {
          realFetcher.getRunRecordMeta(runId);
          Assert.fail("Expected RemoteProgramRunRecordFetcher with caller RUNTIME token to fail");
        } catch (RetryableException expected) {
          Assert.assertTrue(expected.getMessage()
              .contains("Invalid internal access token type; got 'RUNTIME', want 'INTERNAL'"));
          Assert.assertEquals(
              Credential.CREDENTIAL_TYPE_INTERNAL + " " + runtimeTokenStr,
              receivedTokenHeader.get());
        }
        receivedTokenHeader.set(null);

        DefaultHttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        req.headers().set(Constants.Security.Headers.RUNTIME_TOKEN,
            Credential.CREDENTIAL_TYPE_INTERNAL + " " + runtimeTokenStr);

        ValidatedRuntimeRequest validated = validator.validate(runId, req);
        Assert.assertTrue(validated.isRuntimeToken());
        Assert.assertEquals(runId, validated.getTokenRunId());
        Assert.assertEquals(SAMPLE_SCOPES, validated.getRuntimeScopes());
        Assert.assertNotNull(receivedTokenHeader.get());
        Assert.assertNotEquals(
            Credential.CREDENTIAL_TYPE_INTERNAL + " " + runtimeTokenStr,
            receivedTokenHeader.get());

        // Caller's SecurityRequestContext must be restored
        Assert.assertEquals("runtime", SecurityRequestContext.getUserId());
        Assert.assertEquals("10.0.0.9", SecurityRequestContext.getUserIp());
        Assert.assertEquals(callerCred, SecurityRequestContext.getUserCredential());
      } finally {
        registration.cancel();
        appFabricService.stop();
      }
    } finally {
      Services.stopAndWait(secTokenManager);
    }
  }

  @Test
  public void testFailureLoggingStripsQueryAndFragment() throws Exception {
    CConfiguration conf = createSecurityConf(true, false, 60_000L);
    ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf")
        .run(RunIds.generate());
    insertLocalStartingRun(runId, Collections.emptyMap());

    Logger mockLogger = Mockito.mock(Logger.class);
    RuntimeValidationReporter reporter = new RuntimeValidationReporter(
        conf, new NoOpMetricsCollectionService(), mockLogger, TimeProvider.SYSTEM_TIME);
    DirectRuntimeRequestValidator validator = new DirectRuntimeRequestValidator(
        conf, txRunner, new MockProgramRunRecordFetcher(), accessEnforcer, authenticationContext,
        tokenManager, accessTokenCodec, reporter);

    long now = System.currentTimeMillis();
    AccessToken token = signRuntimeToken(GSON.toJson(runId), now, now + 60_000L);
    String authHeader = Credential.CREDENTIAL_TYPE_INTERNAL + " "
        + Base64.getEncoder().encodeToString(accessTokenCodec.encode(token));

    DefaultHttpRequest queryAndFragReq = newGetRequest("/v3Internal/runtime/path?secret=omit#frag");
    queryAndFragReq.headers().set(Constants.Security.Headers.RUNTIME_TOKEN, authHeader);
    validator.validate(runId, queryAndFragReq);

    DefaultHttpRequest fragmentOnlyReq = newGetRequest("/v3Internal/runtime/frag-only#frag");
    fragmentOnlyReq.headers().set(Constants.Security.Headers.RUNTIME_TOKEN, authHeader);
    validator.validate(runId, fragmentOnlyReq);

    Mockito.verify(mockLogger).debug(Mockito.anyString(),
        Mockito.eq("missing_scopes"), Mockito.eq(runId), Mockito.eq(Constants.Service.RUNTIME),
        Mockito.eq("GET"), Mockito.eq("/v3Internal/runtime/path"), Mockito.eq(false));
    Mockito.verify(mockLogger).debug(Mockito.anyString(),
        Mockito.eq("missing_scopes"), Mockito.eq(runId), Mockito.eq(Constants.Service.RUNTIME),
        Mockito.eq("GET"), Mockito.eq("/v3Internal/runtime/frag-only"), Mockito.eq(false));
  }

  // ---- isSameRunExceptVersion ----

  @Test
  public void testIsSameRunExceptVersion() {
    String run = RunIds.generate().getId();
    ProgramRunId v1 = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf").run(run);
    ProgramRunId snapshot = NamespaceId.DEFAULT.app("app", ApplicationId.DEFAULT_VERSION)
        .workflow("wf").run(run);
    ProgramRunId diffRun = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf")
        .run(RunIds.generate());
    ProgramRunId diffProg = NamespaceId.DEFAULT.app("app", "1.0").spark("wf").run(run);
    ProgramRunId diffNs = new NamespaceId("other").app("app", "1.0").workflow("wf").run(run);

    Assert.assertTrue(DirectRuntimeRequestValidator.isSameRunExceptVersion(v1, v1));
    Assert.assertTrue(DirectRuntimeRequestValidator.isSameRunExceptVersion(v1, snapshot));
    Assert.assertFalse(DirectRuntimeRequestValidator.isSameRunExceptVersion(v1, diffRun));
    Assert.assertFalse(DirectRuntimeRequestValidator.isSameRunExceptVersion(v1, diffProg));
    Assert.assertFalse(DirectRuntimeRequestValidator.isSameRunExceptVersion(v1, diffNs));
  }

  // ---- Helpers ----

  private void setupStoreAgain() throws IOException, TableAlreadyExistsException {
    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        new IOModule(),
        new SystemDatasetRuntimeModule().getInMemoryModules(),
        new DataSetsModules().getInMemoryModules(),
        new NamespaceAdminTestModule(),
        new StorageModule(),
        new AuthenticationContextModules().getNoOpModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
            bind(AccessEnforcer.class).to(NoOpAccessController.class);
            bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class);
            bind(Store.class).to(DefaultStore.class);
          }
        });
    StoreDefinition.AppMetadataStore.create(injector.getInstance(StructuredTableAdmin.class));
    txRunner = injector.getInstance(TransactionRunner.class);
  }

  private CConfiguration createSecurityConf(boolean internalAuthEnabled, boolean validationEnabled,
      long pollTimeMs) {
    CConfiguration conf = CConfiguration.copy(cConf);
    conf.setBoolean(Constants.Security.ENABLED, internalAuthEnabled);
    conf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, internalAuthEnabled);
    conf.setBoolean(Constants.RuntimeMonitor.RUNTIME_TOKEN_VALIDATION_ENABLED, validationEnabled);
    conf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, pollTimeMs);
    return conf;
  }

  private DirectRuntimeRequestValidator createValidator(CConfiguration conf,
      ProgramRunRecordFetcher fetcher, MetricsCollectionService metricsService) {
    RuntimeValidationReporter reporter = new RuntimeValidationReporter(conf, metricsService);
    return new DirectRuntimeRequestValidator(conf, txRunner, fetcher, accessEnforcer,
        authenticationContext, tokenManager, accessTokenCodec, reporter);
  }

  private AccessToken signRuntimeToken(String username, long issueTime, long expireTime) {
    return tokenManager.signIdentifier(new UserIdentity(
        username, UserIdentity.IdentifierType.RUNTIME, Collections.emptyList(),
        issueTime, expireTime));
  }

  private DefaultHttpRequest createRuntimeTokenRequest(ProgramRunId tokenRunId) throws IOException {
    long now = System.currentTimeMillis();
    return createTokenRequest(signRuntimeToken(GSON.toJson(tokenRunId), now, now + 60_000L));
  }

  private DefaultHttpRequest createInternalTokenRequest(String username) throws IOException {
    long now = System.currentTimeMillis();
    AccessToken token = tokenManager.signIdentifier(new UserIdentity(
        username, UserIdentity.IdentifierType.INTERNAL, Collections.emptyList(),
        now, now + 60_000L));
    return createTokenRequest(token);
  }

  private DefaultHttpRequest createTokenRequest(AccessToken token) throws IOException {
    DefaultHttpRequest request = newGetRequest("/test?secret=omit");
    String encoded = Base64.getEncoder().encodeToString(accessTokenCodec.encode(token));
    request.headers().set(Constants.Security.Headers.RUNTIME_TOKEN,
        Credential.CREDENTIAL_TYPE_INTERNAL + " " + encoded);
    return request;
  }

  private static DefaultHttpRequest newGetRequest(String uri) {
    return new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
  }

  private void insertLocalStartingRun(ProgramRunId programRunId, Map<String, String> extraSysArgs) {
    Map<String, String> sysArgs = new HashMap<>(extraSysArgs);
    sysArgs.putIfAbsent(SystemArguments.PROFILE_NAME, "system:default");
    TransactionRunners.run(txRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      store.recordProgramProvisioning(programRunId, Collections.emptyMap(), sysArgs,
          createSourceId(1), ARTIFACT_ID);
      store.recordProgramProvisioned(programRunId, 1, createSourceId(2));
      store.recordProgramStart(programRunId, null, sysArgs, createSourceId(3));
    });
  }

  private void insertLocalCompletedRun(ProgramRunId programRunId, Map<String, String> extraSysArgs,
      ProgramRunStatus endStatus) {
    insertLocalStartingRun(programRunId, extraSysArgs);
    TransactionRunners.run(txRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      store.recordProgramStop(programRunId, System.currentTimeMillis(), endStatus, null,
          createSourceId(4));
    });
  }

  private static RunRecordDetail createRunRecord(ProgramRunId programRunId,
      ProgramRunStatus status, @Nullable ProgramRunClusterStatus clusterStatus,
      Map<String, String> extraSysArgs) {
    Map<String, String> sysArgs = new HashMap<>(extraSysArgs);
    sysArgs.putIfAbsent(SystemArguments.PROFILE_NAME, "default");
    sysArgs.putIfAbsent(SystemArguments.PROFILE_PROVISIONER, "native");
    RunRecordDetail.Builder builder = RunRecordDetail.builder()
        .setProgramRunId(programRunId)
        .setStartTime(System.currentTimeMillis())
        .setArtifactId(ARTIFACT_ID)
        .setStatus(status)
        .setSystemArgs(sysArgs)
        .setProfileId(NamespaceId.DEFAULT.profile("native"))
        .setSourceId(new byte[MessageId.RAW_ID_SIZE]);
    if (clusterStatus != null) {
      builder.setCluster(new ProgramRunCluster(clusterStatus, null, 1));
    }
    return builder.build();
  }

  private static byte[] createSourceId(int value) {
    return Bytes.toBytes(value);
  }

  /**
   * Public HTTP handler guarded by {@link InternalAccessEnforcer} for testing remote run-record
   * fetches.
   */
  public static final class EnforcedRunRecordHandler extends AbstractHttpHandler {

    private final AtomicReference<String> receivedTokenHeader;
    private final InternalAccessEnforcer internalAccessEnforcer;
    private final AuthenticationContext authenticationContext;
    private final RunRecordDetail runRecord;

    public EnforcedRunRecordHandler(AtomicReference<String> receivedTokenHeader,
        InternalAccessEnforcer internalAccessEnforcer,
        AuthenticationContext authenticationContext,
        RunRecordDetail runRecord) {
      this.receivedTokenHeader = receivedTokenHeader;
      this.internalAccessEnforcer = internalAccessEnforcer;
      this.authenticationContext = authenticationContext;
      this.runRecord = runRecord;
    }

    @Path(Constants.Gateway.INTERNAL_API_VERSION_3
        + "/namespaces/{ns}/apps/{app}/versions/{version}/{program-type}/{program}/runs/{run}")
    @GET
    public void getRun(HttpRequest req, HttpResponder responder,
        @PathParam("ns") String ns, @PathParam("app") String app,
        @PathParam("version") String version, @PathParam("program-type") String type,
        @PathParam("program") String program, @PathParam("run") String run)
        throws AccessException {
      receivedTokenHeader.set(req.headers().get(Constants.Security.Headers.RUNTIME_TOKEN));
      internalAccessEnforcer.enforce(InstanceId.SELF, authenticationContext.getPrincipal(),
          StandardPermission.GET);
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(runRecord));
    }
  }

  /**
   * Capturing {@link MetricsCollectionService} for asserting failure counters.
   */
  private static final class RecordingMetricsCollectionService
      extends NoOpMetricsCollectionService {

    private final Map<String, Long> counts = new ConcurrentHashMap<>();

    @Override
    public MetricsContext getContext(Map<String, String> tags) {
      MetricsContext delegate = super.getContext(tags);
      return new MetricsContext() {
        @Override
        public MetricsContext childContext(String tagName, String tagValue) {
          return delegate.childContext(tagName, tagValue);
        }

        @Override
        public MetricsContext childContext(Map<String, String> childTags) {
          return delegate.childContext(childTags);
        }

        @Override
        public Map<String, String> getTags() {
          return tags;
        }

        @Override
        public void increment(String metricName, long value) {
          counts.merge(metricName, value, Long::sum);
        }

        @Override
        public void gauge(String metricName, long value) {
          delegate.gauge(metricName, value);
        }

        @Override
        public void event(String metricName, long value) {
          delegate.event(metricName, value);
        }
      };
    }

    Map<String, Long> getCounts() {
      return counts;
    }

    @Nullable
    Long getCount(RuntimeValidationFailure failure) {
      return counts.get(failure.getMetricName());
    }
  }

  /**
   * A mock {@link ProgramRunRecordFetcher} supporting multiple runs, fetch counting, and an
   * optional barrier for concurrency tests.
   */
  private static final class MockProgramRunRecordFetcher implements ProgramRunRecordFetcher {

    private final Map<ProgramRunId, RunRecordDetail> records = new ConcurrentHashMap<>();
    private final Map<ProgramRunId, AtomicInteger> fetchCounts = new ConcurrentHashMap<>();
    private final AtomicInteger totalFetchCount = new AtomicInteger();
    @Nullable
    private CountDownLatch fetchEntered;
    @Nullable
    private CountDownLatch releaseFetch;

    MockProgramRunRecordFetcher putRunRecord(RunRecordDetail runRecord) {
      records.put(runRecord.getProgramRunId(), runRecord);
      return this;
    }

    MockProgramRunRecordFetcher clear() {
      records.clear();
      return this;
    }

    MockProgramRunRecordFetcher withFetchBarrier(CountDownLatch fetchEntered,
        CountDownLatch releaseFetch) {
      this.fetchEntered = fetchEntered;
      this.releaseFetch = releaseFetch;
      return this;
    }

    int getFetchCount(ProgramRunId runId) {
      AtomicInteger count = fetchCounts.get(runId);
      return count == null ? 0 : count.get();
    }

    int getTotalFetchCount() {
      return totalFetchCount.get();
    }

    @Override
    public RunRecordDetail getRunRecordMeta(ProgramRunId runId) throws NotFoundException {
      totalFetchCount.incrementAndGet();
      fetchCounts.computeIfAbsent(runId, k -> new AtomicInteger()).incrementAndGet();
      if (fetchEntered != null) {
        fetchEntered.countDown();
      }
      if (releaseFetch != null) {
        try {
          releaseFetch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      RunRecordDetail record = records.get(runId);
      if (record == null) {
        throw new NotFoundException(runId);
      }
      return record;
    }

    @Override
    public RunRecordDetail getRunRecordMeta(ProgramReference programRef, String runId)
        throws NotFoundException {
      for (RunRecordDetail record : records.values()) {
        if (Objects.equals(programRef, record.getProgramRunId().getParent().getProgramReference())
            && Objects.equals(runId, record.getProgramRunId().getRun())) {
          return record;
        }
      }
      throw new NotFoundException(String.format("No run record found for program %s and runID: %s",
          programRef, runId));
    }
  }
}
