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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.ServiceException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.encryption.guice.UserCredentialAeadEncryptionModule;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.service.Services;
import io.cdap.cdap.common.twill.NoopTwillRunnerService;
import io.cdap.cdap.gateway.handlers.meta.RemotePrivilegesHandler;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.ProgramLaunchConfig;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeScopes;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.RemoteAccessEnforcer;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.impersonation.DefaultImpersonator;
import io.cdap.cdap.security.spi.authorization.AccessControllerSpi;
import io.cdap.cdap.security.spi.authorization.NoOpAccessController;
import io.cdap.cdap.security.spi.authorization.PermissionManager;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.FullHttpRequest;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillController;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for runtime token minting in {@link RemoteExecutionTwillRunnerService}, container
 * authorization config in {@link DistributedProgramRunner}, and remote enforcement behavior when
 * carrying a {@link UserIdentity.IdentifierType#RUNTIME} token.
 */
public class RuntimeTokenIssuanceTest {

  private static final Gson GSON = new Gson();
  private static final ProgramRunId RUN_ID =
      new NamespaceId("ns1").app("pipe1", "1.0").workflow("wf1").run(RunIds.generate());

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

  private CConfiguration cConf;
  private Injector securityInjector;
  private TokenManager tokenManager;
  private AccessTokenCodec accessTokenCodec;
  private RemoteExecutionTwillRunnerService runnerService;
  private LocalLocationFactory locationFactory;

  @Before
  public void setUp() throws Exception {
    cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    cConf.set(Constants.RuntimeMonitor.MONITOR_URL, "https://monitor.example.com");
    cConf.set(Constants.RuntimeMonitor.MONITOR_TYPE_PREFIX + "gcp-dataproc",
        RuntimeMonitorType.URL.name());

    AccessControllerInstantiator noOpInstantiator = Mockito.mock(AccessControllerInstantiator.class);
    Mockito.when(noOpInstantiator.get()).thenReturn(Mockito.mock(AccessControllerSpi.class));

    securityInjector = Guice.createInjector(
        new ConfigModule(cConf, new Configuration(), SConfiguration.create()),
        new IOModule(),
        new CoreSecurityRuntimeModule().getInMemoryModules(),
        new AuthenticationContextModules().getMasterModule(),
        new UserCredentialAeadEncryptionModule(),
        new AuthorizationEnforcementModule().getMasterModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
            bind(AccessControllerInstantiator.class).toInstance(noOpInstantiator);
            bind(PermissionManager.class).to(NoOpAccessController.class);
          }
        });

    tokenManager = securityInjector.getInstance(TokenManager.class);
    Services.startAndWait(tokenManager);
    accessTokenCodec = securityInjector.getInstance(AccessTokenCodec.class);

    File rootDir = tmpFolder.newFolder("loc");
    locationFactory = new LocalLocationFactory(rootDir);
    runnerService = new RemoteExecutionTwillRunnerService(
        cConf,
        new Configuration(),
        new InMemoryDiscoveryService(),
        locationFactory,
        Mockito.mock(ProvisioningService.class),
        Mockito.mock(ProgramStateWriter.class),
        Mockito.mock(TransactionRunner.class),
        accessTokenCodec,
        tokenManager);
  }

  @After
  public void tearDown() {
    if (tokenManager != null) {
      Services.stopAndWait(tokenManager);
    }
  }

  @Test
  public void testGenerateAndSaveRuntimeTokenScopedLegacyAndSshFallback() throws Exception {
    String scopesJson = new RuntimeScopes(
        Collections.singleton("conn1"), Collections.emptySet(),
        Collections.singleton("sec1"), false).toJson();

    // Scopes present + URL monitor type + internal auth on -> RUNTIME token with Gson ProgramRunId
    ProgramOptions urlScopedOptions = new SimpleProgramOptions(
        RUN_ID.getParent(),
        new BasicArguments(ImmutableMap.of(
            SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc",
            ProgramOptionConstants.RUNTIME_SCOPES, scopesJson)),
        new BasicArguments());
    Location scopedKeysDir = locationFactory.create("scoped-keys");
    scopedKeysDir.mkdirs();
    Location scopedTokenFile = runnerService.generateAndSaveRuntimeToken(
        cConf, RUN_ID, urlScopedOptions, scopedKeysDir);

    AccessToken runtimeToken = decodeToken(scopedTokenFile);
    tokenManager.validateSecret(runtimeToken);
    UserIdentity runtimeIdentity = runtimeToken.getIdentifier();
    Assert.assertEquals(UserIdentity.IdentifierType.RUNTIME, runtimeIdentity.getIdentifierType());
    Assert.assertEquals(RUN_ID, GSON.fromJson(runtimeIdentity.getUsername(), ProgramRunId.class));
    long ttlMillis = runtimeIdentity.getExpireTimestamp()
        - runtimeIdentity.getIssueTimestamp();
    Assert.assertEquals(TimeUnit.DAYS.toMillis(365), ttlMillis);

    // No scopes (issuance disabled) -> legacy INTERNAL token
    ProgramOptions noScopesOptions = new SimpleProgramOptions(
        RUN_ID.getParent(),
        new BasicArguments(Collections.singletonMap(
            SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc")),
        new BasicArguments());
    Location legacyKeysDir = locationFactory.create("legacy-keys");
    legacyKeysDir.mkdirs();
    AccessToken legacyToken = decodeToken(runnerService.generateAndSaveRuntimeToken(
        cConf, RUN_ID, noScopesOptions, legacyKeysDir));
    tokenManager.validateSecret(legacyToken);
    Assert.assertEquals(UserIdentity.IdentifierType.INTERNAL,
        legacyToken.getIdentifier().getIdentifierType());
    Assert.assertEquals(Constants.Security.Authentication.RUNTIME_IDENTITY,
        legacyToken.getIdentifier().getUsername());

    // User arg system.runtime.monitor.type=SSH on a URL-configured provisioner still mints RUNTIME
    ProgramOptions userArgSshWithScopes = new SimpleProgramOptions(
        RUN_ID.getParent(),
        new BasicArguments(ImmutableMap.of(
            SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc",
            ProgramOptionConstants.RUNTIME_SCOPES, scopesJson)),
        new BasicArguments(Collections.singletonMap(
            SystemArguments.RUNTIME_MONITOR_TYPE, RuntimeMonitorType.SSH.name())));
    Location userArgSshKeysDir = locationFactory.create("user-arg-ssh-keys");
    userArgSshKeysDir.mkdirs();
    AccessToken userArgSshToken = decodeToken(runnerService.generateAndSaveRuntimeToken(
        cConf, RUN_ID, userArgSshWithScopes, userArgSshKeysDir));
    tokenManager.validateSecret(userArgSshToken);
    Assert.assertEquals(UserIdentity.IdentifierType.RUNTIME,
        userArgSshToken.getIdentifier().getIdentifierType());
    Assert.assertEquals(RUN_ID,
        GSON.fromJson(userArgSshToken.getIdentifier().getUsername(), ProgramRunId.class));

    // Operator-configured SSH provisioner in cConf (even with user arg URL) -> legacy INTERNAL
    CConfiguration sshOperatorConf = CConfiguration.copy(cConf);
    sshOperatorConf.set(Constants.RuntimeMonitor.MONITOR_TYPE_PREFIX + "gcp-dataproc",
        RuntimeMonitorType.SSH.name());
    ProgramOptions userArgUrlWithScopes = new SimpleProgramOptions(
        RUN_ID.getParent(),
        new BasicArguments(ImmutableMap.of(
            SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc",
            ProgramOptionConstants.RUNTIME_SCOPES, scopesJson)),
        new BasicArguments(Collections.singletonMap(
            SystemArguments.RUNTIME_MONITOR_TYPE, RuntimeMonitorType.URL.name())));
    Location sshKeysDir = locationFactory.create("ssh-keys");
    sshKeysDir.mkdirs();
    AccessToken sshToken = decodeToken(runnerService.generateAndSaveRuntimeToken(
        sshOperatorConf, RUN_ID, userArgUrlWithScopes, sshKeysDir));
    tokenManager.validateSecret(sshToken);
    Assert.assertEquals(UserIdentity.IdentifierType.INTERNAL,
        sshToken.getIdentifier().getIdentifierType());
    Assert.assertEquals(Constants.Security.Authentication.RUNTIME_IDENTITY,
        sshToken.getIdentifier().getUsername());

    // Recovery ProgramOptions resolves scoped user args when operator config omits monitor type
    RunRecordDetail recoveryRecord = RunRecordDetail.builder()
        .setProgramRunId(RUN_ID)
        .setStartTime(1000L)
        .setStatus(ProgramRunStatus.RUNNING)
        .setSystemArgs(ImmutableMap.of(
            SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc",
            ProgramOptionConstants.RUNTIME_SCOPES, scopesJson))
        .setProperties(Collections.singletonMap("runtimeArgs", GSON.toJson(
            Collections.singletonMap(
                "workflow." + RUN_ID.getProgram() + "." + SystemArguments.RUNTIME_MONITOR_TYPE,
                RuntimeMonitorType.URL.name()))))
        .setSourceId(new byte[]{1})
        .build();
    CConfiguration unsetMonitorTypeConf = CConfiguration.copy(cConf);
    unsetMonitorTypeConf.unset(Constants.RuntimeMonitor.MONITOR_TYPE_PREFIX + "gcp-dataproc");
    ProgramOptions recoveryOptions =
        RemoteExecutionTwillRunnerService.createProgramOptionsFromRunRecord(recoveryRecord);
    Assert.assertEquals(RuntimeMonitorType.URL,
        SystemArguments.getRuntimeMonitorType(unsetMonitorTypeConf, recoveryOptions));
  }

  @Test
  public void testContainerConfDisablesAuthzOnlyForRuntimeTokenRuns() throws Exception {
    File baseDir = tmpFolder.newFolder("runner");
    TestRunner isolatedRunner = new TestRunner(cConf, baseDir, ClusterMode.ISOLATED);

    String scopesJson = RuntimeScopes.wildcardAll().toJson();
    ProgramOptions urlScoped = new SimpleProgramOptions(
        RUN_ID.getParent(),
        new BasicArguments(ImmutableMap.of(
            SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc",
            ProgramOptionConstants.RUNTIME_SCOPES, scopesJson)),
        new BasicArguments());
    ProgramOptions userArgSshScoped = new SimpleProgramOptions(
        RUN_ID.getParent(),
        new BasicArguments(ImmutableMap.of(
            SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc",
            ProgramOptionConstants.RUNTIME_SCOPES, scopesJson)),
        new BasicArguments(Collections.singletonMap(
            SystemArguments.RUNTIME_MONITOR_TYPE, RuntimeMonitorType.SSH.name())));

    // Isolated + usesRuntimeToken -> security.authorization.enabled=false
    CConfiguration shippedForRuntimeToken = isolatedRunner.buildContainerConf(cConf, urlScoped);
    Assert.assertFalse(shippedForRuntimeToken.getBoolean(Constants.Security.Authorization.ENABLED));

    // User arg SSH on URL-configured provisioner still uses RUNTIME token -> authz disabled
    CConfiguration shippedForUserArgSsh =
        isolatedRunner.buildContainerConf(cConf, userArgSshScoped);
    Assert.assertFalse(shippedForUserArgSsh.getBoolean(Constants.Security.Authorization.ENABLED));

    // Operator-configured SSH provisioner (even with user arg URL) -> authorization left enabled
    CConfiguration sshOperatorConf = CConfiguration.copy(cConf);
    sshOperatorConf.set(Constants.RuntimeMonitor.MONITOR_TYPE_PREFIX + "gcp-dataproc",
        RuntimeMonitorType.SSH.name());
    ProgramOptions userArgUrlScoped = new SimpleProgramOptions(
        RUN_ID.getParent(),
        new BasicArguments(ImmutableMap.of(
            SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc",
            ProgramOptionConstants.RUNTIME_SCOPES, scopesJson)),
        new BasicArguments(Collections.singletonMap(
            SystemArguments.RUNTIME_MONITOR_TYPE, RuntimeMonitorType.URL.name())));
    CConfiguration shippedForOperatorSsh =
        isolatedRunner.buildContainerConf(sshOperatorConf, userArgUrlScoped);
    Assert.assertTrue(shippedForOperatorSsh.getBoolean(Constants.Security.Authorization.ENABLED));

    // No scopes -> authorization left enabled
    ProgramOptions noScopes = new SimpleProgramOptions(
        RUN_ID.getParent(),
        new BasicArguments(Collections.singletonMap(
            SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc")),
        new BasicArguments());
    CConfiguration shippedWithoutScopes = isolatedRunner.buildContainerConf(cConf, noScopes);
    Assert.assertTrue(shippedWithoutScopes.getBoolean(Constants.Security.Authorization.ENABLED));

    // ON_PREMISE cluster mode -> authorization left enabled
    TestRunner onPremiseRunner = new TestRunner(cConf, baseDir, ClusterMode.ON_PREMISE);
    CConfiguration shippedOnPremise = onPremiseRunner.buildContainerConf(cConf, urlScoped);
    Assert.assertTrue(shippedOnPremise.getBoolean(Constants.Security.Authorization.ENABLED));

    // Internal auth disabled -> authorization left enabled even with scopes and URL monitor type
    CConfiguration noInternalAuthConf = CConfiguration.copy(cConf);
    noInternalAuthConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, false);
    CConfiguration shippedWithoutInternalAuth =
        isolatedRunner.buildContainerConf(noInternalAuthConf, urlScoped);
    Assert.assertTrue(
        shippedWithoutInternalAuth.getBoolean(Constants.Security.Authorization.ENABLED));
  }

  @Test
  public void testRemoteEnforceWithRuntimeTokenFailsAndShippedConfigBypassesCall()
      throws Exception {
    // Wire RemotePrivilegesHandler with AuthorizationEnforcementModule.getMasterModule()
    // and in-memory TokenManager.
    RemotePrivilegesHandler privilegesHandler =
        securityInjector.getInstance(RemotePrivilegesHandler.class);
    AtomicInteger enforceCalls = new AtomicInteger();
    CountingPrivilegesHandler countingHandler =
        new CountingPrivilegesHandler(privilegesHandler, enforceCalls);

    NettyHttpService httpService = NettyHttpService.builder("privileges-test")
        .setHttpHandlers(countingHandler)
        .build();
    httpService.start();
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    Cancellable registration = discoveryService.register(
        URIScheme.createDiscoverable(Constants.Service.APP_FABRIC_HTTP, httpService));

    try {
      String scopesJson = RuntimeScopes.wildcardAll().toJson();
      ProgramOptions urlScopedOptions = new SimpleProgramOptions(
          RUN_ID.getParent(),
          new BasicArguments(ImmutableMap.of(
              SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc",
              ProgramOptionConstants.RUNTIME_SCOPES, scopesJson)),
          new BasicArguments());
      ProgramOptions noScopesOptions = new SimpleProgramOptions(
          RUN_ID.getParent(),
          new BasicArguments(Collections.singletonMap(
              SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc")),
          new BasicArguments());

      // Mint both a legacy INTERNAL token (positive control) and a RUNTIME token
      Location legacyKeysDir = locationFactory.create("enforce-legacy-keys");
      legacyKeysDir.mkdirs();
      String legacyEncodedToken = readTokenString(runnerService.generateAndSaveRuntimeToken(
          cConf, RUN_ID, noScopesOptions, legacyKeysDir));
      Principal legacyPrincipal = new Principal(
          Constants.Security.Authentication.RUNTIME_IDENTITY, Principal.PrincipalType.USER,
          new Credential(legacyEncodedToken, Credential.CredentialType.INTERNAL));

      Location runtimeKeysDir = locationFactory.create("enforce-keys");
      runtimeKeysDir.mkdirs();
      String runtimeEncodedToken = readTokenString(runnerService.generateAndSaveRuntimeToken(
          cConf, RUN_ID, urlScopedOptions, runtimeKeysDir));
      Principal runtimePrincipal = new Principal(
          GSON.toJson(RUN_ID), Principal.PrincipalType.USER,
          new Credential(runtimeEncodedToken, Credential.CredentialType.INTERNAL));
      DatasetId datasetId = new NamespaceId("ns1").dataset("ds1");

      RemoteClientFactory clientFactory = new RemoteClientFactory(discoveryService, setter -> { });

      // Positive control: legacy INTERNAL token succeeds against RemotePrivilegesHandler
      RemoteAccessEnforcer enabledEnforcer = new RemoteAccessEnforcer(cConf, clientFactory);
      enabledEnforcer.enforce(datasetId, legacyPrincipal, StandardPermission.GET);
      Assert.assertEquals(1, enforceCalls.get());

      // With authorization enabled in container config, RemoteAccessEnforcer sends the RUNTIME
      // token inside the enforce request body and master's InternalAccessEnforcer rejects it
      // because the token type is RUNTIME rather than INTERNAL (HTTP 500 -> ServiceException).
      try {
        enabledEnforcer.enforce(datasetId, runtimePrincipal, StandardPermission.GET);
        Assert.fail("Expected enforce with RUNTIME token credential in body to fail");
      } catch (ServiceException expected) {
        Assert.assertTrue(expected.getMessage().contains("Invalid internal access token type"));
        Assert.assertTrue(expected.getMessage().contains("RUNTIME"));
      }
      Assert.assertEquals(2, enforceCalls.get());

      // With the container config shipped by DistributedProgramRunner (authorization disabled),
      // RemoteAccessEnforcer makes zero HTTP calls and succeeds for enforce, enforceOnParent,
      // and isVisible.
      TestRunner isolatedRunner = new TestRunner(
          cConf, tmpFolder.newFolder("enforce-runner"), ClusterMode.ISOLATED);
      CConfiguration shippedConf = isolatedRunner.buildContainerConf(cConf, urlScopedOptions);
      Assert.assertFalse(shippedConf.getBoolean(Constants.Security.Authorization.ENABLED));

      enforceCalls.set(0);
      RemoteAccessEnforcer shippedEnforcer = new RemoteAccessEnforcer(shippedConf, clientFactory);
      shippedEnforcer.enforce(datasetId, runtimePrincipal, StandardPermission.GET);
      shippedEnforcer.enforceOnParent(
          EntityType.DATASET, datasetId.getParent(), runtimePrincipal, StandardPermission.GET);
      Assert.assertEquals(Collections.singleton(datasetId),
          shippedEnforcer.isVisible(Collections.singleton(datasetId), runtimePrincipal));
      Assert.assertEquals(0, enforceCalls.get());
    } finally {
      registration.cancel();
      httpService.stop();
    }
  }

  private String readTokenString(Location tokenFile) throws Exception {
    try (InputStream is = tokenFile.getInputStream()) {
      return new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8);
    }
  }

  private AccessToken decodeToken(Location tokenFile) throws Exception {
    byte[] base64Bytes;
    try (InputStream is = tokenFile.getInputStream()) {
      base64Bytes = ByteStreams.toByteArray(is);
    }
    return accessTokenCodec.decode(Base64.getDecoder().decode(base64Bytes));
  }

  private static final class TestRunner extends DistributedProgramRunner {

    private TestRunner(CConfiguration cConf, File baseDir, ClusterMode clusterMode) {
      super(cConf, new Configuration(), new DefaultImpersonator(cConf, null),
          clusterMode, new NoopTwillRunnerService(), new LocalLocationFactory(baseDir));
    }

    private CConfiguration buildContainerConf(CConfiguration conf, ProgramOptions options) {
      return createContainerCConf(conf, options);
    }

    @Override
    public ProgramController createProgramController(ProgramRunId programRunId,
        TwillController twillController) {
      return null;
    }

    @Override
    protected void setupLaunchConfig(ProgramLaunchConfig launchConfig, Program program,
        ProgramOptions options, CConfiguration cConf, Configuration hConf, File tempDir) {
    }
  }

  /**
   * Wrapper around {@link RemotePrivilegesHandler} that counts incoming enforce requests.
   */
  @Path("/v1/execute")
  public static final class CountingPrivilegesHandler extends AbstractHttpHandler {

    private final RemotePrivilegesHandler delegate;
    private final AtomicInteger enforceCalls;

    public CountingPrivilegesHandler(RemotePrivilegesHandler delegate,
        AtomicInteger enforceCalls) {
      this.delegate = delegate;
      this.enforceCalls = enforceCalls;
    }

    @POST
    @Path("/enforce")
    public void enforce(FullHttpRequest request, HttpResponder responder) throws Exception {
      enforceCalls.incrementAndGet();
      delegate.enforce(request, responder);
    }

    @POST
    @Path("/isVisible")
    public void isVisible(FullHttpRequest request, HttpResponder responder) throws Exception {
      enforceCalls.incrementAndGet();
      delegate.isVisible(request, responder);
    }
  }
}
