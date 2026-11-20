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

package io.cdap.cdap.internal.app.preview;

import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRequestQueue;
import io.cdap.cdap.app.preview.PreviewRunner;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.encryption.NoOpAeadCipher;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.internal.remote.NoOpRemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerTwillRunnable;
import io.cdap.cdap.internal.app.worker.sidecar.GcpMetadataHttpHandlerInternal;
import io.cdap.cdap.master.spi.twill.SecretDisk;
import io.cdap.cdap.master.spi.twill.SecureTwillPreparer;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.GcpMetadataTaskContext;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.NettyHttpService;
import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit test for {@link PreviewRunnerService} and {@link DistributedPreviewManager}.
 */
public class PreviewRunnerServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final Gson GSON = new Gson();

  private CConfiguration createCConf() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setLong(Constants.Preview.REQUEST_POLL_DELAY_MILLIS, 200);
    return cConf;
  }

  @Test
  public void testStartAndStop() throws InterruptedException, ExecutionException, TimeoutException {
    MockPreviewRunner mockRunner = new MockPreviewRunner();
    MockPreviewRequestFetcher fetcher = new MockPreviewRequestFetcher();
    PreviewRunnerService runnerService =
        new PreviewRunnerService(createCConf(), fetcher, mockRunner);
    runnerService.startAndWait();

    Tasks.waitFor(true, () -> fetcher.fetchCount.get() > 0,
        5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    runnerService.stopAndWait();
    Tasks.waitFor(Service.State.TERMINATED, runnerService::state,
        5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testStopPreview() throws InterruptedException, ExecutionException, TimeoutException {
    MockPreviewRunner mockRunner = new MockPreviewRunner();
    MockPreviewRequestFetcher fetcher = new MockPreviewRequestFetcher();
    PreviewRunnerService runnerService =
        new PreviewRunnerService(createCConf(), fetcher, mockRunner);
    runnerService.startAndWait();

    ProgramId programId = NamespaceId.DEFAULT.app("app").program(ProgramType.WORKFLOW, "workflow");
    fetcher.addRequest(new PreviewRequest(programId, null, null));

    Tasks.waitFor(true, () -> mockRunner.requests.get(programId) != null,
        5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    runnerService.stopAndWait();
    Tasks.waitFor(PreviewStatus.Status.KILLED,
        () -> mockRunner.requests.get(programId).status.getStatus(),
        5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    Tasks.waitFor(Service.State.TERMINATED, runnerService::state,
        5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testMaxRuns()
      throws InterruptedException, ExecutionException, TimeoutException, NotFoundException {
    CConfiguration cConf = createCConf();
    cConf.setInt(Constants.Preview.MAX_RUNS, 1);

    MockPreviewRunner mockRunner = new MockPreviewRunner();
    MockPreviewRequestFetcher fetcher = new MockPreviewRequestFetcher();
    PreviewRunnerService runnerService = new PreviewRunnerService(cConf, fetcher, mockRunner);
    runnerService.startAndWait();

    ProgramId programId = NamespaceId.DEFAULT.app("app").program(ProgramType.WORKFLOW, "workflow");
    fetcher.addRequest(new PreviewRequest(programId, null, null));
    Tasks.waitFor(true, () -> mockRunner.requests.get(programId) != null,
        50, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // Finish the preview run and the runner service should terminate since max runs == 1.
    mockRunner.complete(programId);
    Tasks.waitFor(Service.State.TERMINATED, runnerService::state,
        5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testMultiRunPrincipalIsolationDoesNotLeakUserCredentialsAcrossRuns()
      throws Exception {
    CConfiguration cConf = createCConf();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setInt(Constants.Preview.MAX_RUNS, 2);
    // Engage sidecar locking with STRICT=true alone (NAMESPACED_SERVICE_ACCOUNTS=false).
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), false);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.getFeatureFlagString(), true);

    NettyHttpService sidecarHttp = new CommonNettyHttpServiceBuilder(
        cConf, "test-preview-multi-run", new NoOpMetricsCollectionService(), true,
        auditLogContexts -> {}, new NoOpAeadCipher())
        .setHost(InetAddress.getLoopbackAddress().getHostName())
        .setPort(0)
        .setHttpHandlers(new GcpMetadataHttpHandlerInternal(
            cConf, Mockito.mock(RemoteClientFactory.class), new NoOpRemoteAuthenticator()))
        .build();
    sidecarHttp.start();
    try {
      cConf.setInt(Constants.ArtifactLocalizer.PORT, sidecarHttp.getBindAddress().getPort());

      MockPreviewRunner mockRunner = new MockPreviewRunner();
      MockPreviewRequestFetcher fetcher = new MockPreviewRequestFetcher();
      PreviewRunnerService runnerService = new PreviewRunnerService(cConf, fetcher, mockRunner);
      runnerService.startAndWait();

      ProgramId aliceProgram =
          new NamespaceId("ns_alice").app("app1").program(ProgramType.WORKFLOW, "wf1");
      Credential aliceCred = new Credential("alice-bearer-token", Credential.CredentialType.EXTERNAL);
      Principal alicePrincipal = new Principal("alice", Principal.PrincipalType.USER, aliceCred);

      final ProgramId bobProgram =
          new NamespaceId("ns_bob").app("app2").program(ProgramType.WORKFLOW, "wf2");
      final Credential bobCred =
          new Credential("bob-bearer-token", Credential.CredentialType.EXTERNAL);
      final Principal bobPrincipal = new Principal("bob", Principal.PrincipalType.USER, bobCred);

      // Run 1: alice
      fetcher.addRequest(new PreviewRequest(aliceProgram, null, alicePrincipal));
      Tasks.waitFor(true, () -> mockRunner.requests.get(aliceProgram) != null,
          5, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
      Assert.assertEquals("alice", mockRunner.requests.get(aliceProgram).capturedUserId);
      Assert.assertEquals(aliceCred, mockRunner.requests.get(aliceProgram).capturedCredential);
      mockRunner.complete(aliceProgram);

      // Run 2: bob (must see bob's identity and credential, never alice's)
      fetcher.addRequest(new PreviewRequest(bobProgram, null, bobPrincipal));
      Tasks.waitFor(true, () -> mockRunner.requests.get(bobProgram) != null,
          5, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
      Assert.assertEquals("bob", mockRunner.requests.get(bobProgram).capturedUserId);
      Assert.assertEquals(bobCred, mockRunner.requests.get(bobProgram).capturedCredential);
      mockRunner.complete(bobProgram);

      Tasks.waitFor(Service.State.TERMINATED, runnerService::state,
          5, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
    } finally {
      sidecarHttp.stop();
    }
  }

  @Test
  public void testPreviewLocksAndUnlocksSidecarMetadataContext() throws Exception {
    CConfiguration cConf = createCConf();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setInt(Constants.Preview.MAX_RUNS, 1);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), true);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.getFeatureFlagString(), true);

    NettyHttpService sidecarHttp = new CommonNettyHttpServiceBuilder(
        cConf, "test-preview-sidecar", new NoOpMetricsCollectionService(), true,
        auditLogContexts -> {}, new NoOpAeadCipher())
        .setHost(InetAddress.getLoopbackAddress().getHostName())
        .setPort(0)
        .setHttpHandlers(new GcpMetadataHttpHandlerInternal(
            cConf, Mockito.mock(RemoteClientFactory.class), new NoOpRemoteAuthenticator()))
        .build();
    sidecarHttp.start();
    try {
      int sidecarPort = sidecarHttp.getBindAddress().getPort();
      cConf.setInt(Constants.ArtifactLocalizer.PORT, sidecarPort);
      final String sidecarBase = String.format("http://%s:%d",
          InetAddress.getLoopbackAddress().getHostName(), sidecarPort);

      MockPreviewRunner mockRunner = new MockPreviewRunner();
      MockPreviewRequestFetcher fetcher = new MockPreviewRequestFetcher();
      PreviewRunnerService runnerService = new PreviewRunnerService(cConf, fetcher, mockRunner);
      runnerService.startAndWait();

      ProgramId programId =
          new NamespaceId("ns_secure").app("app").program(ProgramType.WORKFLOW, "workflow");
      fetcher.addRequest(new PreviewRequest(programId, null, null));
      Tasks.waitFor(true, () -> mockRunner.requests.get(programId) != null,
          5, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);

      // While preview is running, sidecar is LOCKED: competing PUT and token-less DELETE return 403.
      HttpResponse hijackPut = HttpRequests.execute(
          HttpRequest.put(new URL(sidecarBase + "/set-context"))
              .withBody(GSON.toJson(
                  new GcpMetadataTaskContext("ns_hijack", "mallory", "127.0.0.1", null)))
              .build());
      Assert.assertEquals(403, hijackPut.getResponseCode());

      HttpResponse hijackClear = HttpRequests.execute(
          HttpRequest.delete(new URL(sidecarBase + "/clear-context")).build());
      Assert.assertEquals(403, hijackClear.getResponseCode());

      // Complete preview: PreviewRunnerService unlocks sidecar using stack-local unlockToken.
      mockRunner.complete(programId);
      Tasks.waitFor(Service.State.TERMINATED, runnerService::state,
          5, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);

      // Sidecar is back in IDLE state so subsequent PUT /set-context succeeds with 200.
      HttpResponse relockPut = HttpRequests.execute(
          HttpRequest.put(new URL(sidecarBase + "/set-context"))
              .withBody(GSON.toJson(
                  new GcpMetadataTaskContext("ns_next", "bob", "127.0.0.1", null)))
              .build());
      Assert.assertEquals(200, relockPut.getResponseCode());
    } finally {
      sidecarHttp.stop();
    }
  }

  @Test
  public void testClearMetadataContextFailureTerminatesPreviewRunnerCleanly() throws Exception {
    CConfiguration cConf = createCConf();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    // Set maxRuns = 0 (unlimited) to prove clear failure alone sets terminated = true.
    cConf.setInt(Constants.Preview.MAX_RUNS, 0);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), true);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.getFeatureFlagString(), true);

    NettyHttpService sidecarHttp = new CommonNettyHttpServiceBuilder(
        cConf, "test-preview-sidecar-fail", new NoOpMetricsCollectionService(), true,
        auditLogContexts -> {}, new NoOpAeadCipher())
        .setHost(InetAddress.getLoopbackAddress().getHostName())
        .setPort(0)
        .setHttpHandlers(new GcpMetadataHttpHandlerInternal(
            cConf, Mockito.mock(RemoteClientFactory.class), new NoOpRemoteAuthenticator()))
        .build();
    sidecarHttp.start();
    cConf.setInt(Constants.ArtifactLocalizer.PORT, sidecarHttp.getBindAddress().getPort());

    MockPreviewRunner mockRunner = new MockPreviewRunner();
    MockPreviewRequestFetcher fetcher = new MockPreviewRequestFetcher();
    PreviewRunnerService runnerService = new PreviewRunnerService(cConf, fetcher, mockRunner);
    runnerService.startAndWait();

    ProgramId programId =
        new NamespaceId("ns_secure").app("app").program(ProgramType.WORKFLOW, "workflow");
    fetcher.addRequest(new PreviewRequest(programId, null, null));
    Tasks.waitFor(true, () -> mockRunner.requests.get(programId) != null,
        5, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);

    // Stop sidecar before completing preview so clearGcpMetadataTaskContext fails in finally.
    sidecarHttp.stop();
    mockRunner.complete(programId);

    Tasks.waitFor(Service.State.TERMINATED, runnerService::state,
        5, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testDistributedPreviewManagerStrictFlagAloneDisablesLogPublisherAndWorkerSecret()
      throws Exception {
    CConfiguration cConf = createCConf();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Logging.LOG_PUBLISHER_ENABLED, true);
    cConf.setBoolean(Constants.Twill.Security.WORKER_MOUNT_SECRET, true);
    cConf.set(Constants.Security.SSL.INTERNAL_CERT_PATH, "/etc/cdap/security/internal.jks");
    cConf.setInt(Constants.ArtifactLocalizer.PORT, 11021);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), false);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.getFeatureFlagString(), true);

    AtomicReference<CConfiguration> localizedConfRef = new AtomicReference<>();
    TwillRunner twillRunner = Mockito.mock(TwillRunner.class);
    SecureTwillPreparer preparer = Mockito.mock(
        SecureTwillPreparer.class, Mockito.RETURNS_SELF);
    TwillController twillController = Mockito.mock(TwillController.class);
    Mockito.when(twillRunner.lookup(PreviewRunnerTwillApplication.NAME))
        .thenReturn(Collections.emptyList());
    Mockito.when(twillRunner.prepare(Mockito.any(TwillApplication.class))).thenAnswer(inv -> {
      TwillApplication app = inv.getArgument(0);
      TwillSpecification spec = app.configure();
      LocalFile cConfFile = spec.getRunnables()
          .get(PreviewRunnerTwillRunnable.class.getSimpleName())
          .getLocalFiles().stream()
          .filter(f -> "cConf.xml".equals(f.getName()))
          .findFirst()
          .orElseThrow(() -> new AssertionError("Missing localized cConf.xml"));
      URI uri = cConfFile.getURI();
      localizedConfRef.set(CConfiguration.create(new File(uri).toURI().toURL()));
      return preparer;
    });
    Mockito.when(preparer.start(Mockito.anyLong(), Mockito.any())).thenReturn(twillController);

    InMemoryDiscoveryService discovery = new InMemoryDiscoveryService();
    DistributedPreviewManager manager = new DistributedPreviewManager(
        cConf, new Configuration(), discovery,
        Mockito.mock(DatasetFramework.class),
        Mockito.mock(TransactionSystemClient.class),
        Mockito.mock(AccessControllerInstantiator.class),
        Mockito.mock(AccessEnforcer.class),
        Mockito.mock(AuthenticationContext.class),
        Mockito.mock(LevelDBTableService.class),
        cConf, new Configuration(), SConfiguration.create(),
        Mockito.mock(PreviewRequestQueue.class),
        Mockito.mock(PreviewStore.class),
        Mockito.mock(PreviewRunStopper.class),
        Mockito.mock(MessagingService.class),
        new NoOpMetricsCollectionService(),
        Mockito.mock(PreviewDataCleanupService.class),
        twillRunner, new NoOpAeadCipher());

    manager.run();

    CConfiguration localizedConf = localizedConfRef.get();
    Assert.assertNotNull(localizedConf);
    Assert.assertFalse(localizedConf.getBoolean(Constants.Logging.LOG_PUBLISHER_ENABLED));
    Assert.assertNull(localizedConf.get(Constants.Security.SSL.INTERNAL_CERT_PATH));
    Assert.assertTrue(cConf.getBoolean(Constants.Logging.LOG_PUBLISHER_ENABLED));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> envCaptor = ArgumentCaptor.forClass(Map.class);
    Mockito.verify(preparer).withEnv(
        Mockito.eq(PreviewRunnerTwillRunnable.class.getSimpleName()), envCaptor.capture());
    Assert.assertEquals(
        InetAddress.getLoopbackAddress().getHostName() + ":11021",
        envCaptor.getValue().get(Constants.Preview.GCE_METADATA_HOST_ENV_VAR));
    Mockito.verify(preparer)
        .withNamespacedWorkloadIdentity(PreviewRunnerTwillRunnable.class.getSimpleName());
    Mockito.verify(preparer).withSecretDisk(
        Mockito.eq(ArtifactLocalizerTwillRunnable.class.getSimpleName()),
        Mockito.any(SecretDisk.class));
    Mockito.verify(preparer, Mockito.never()).withSecretDisk(
        Mockito.eq(PreviewRunnerTwillRunnable.class.getSimpleName()),
        Mockito.any(SecretDisk.class));
  }

  /**
   * A mocking {@link PreviewRunner} for unit testing.
   */
  private static final class MockPreviewRunner implements PreviewRunner {

    private final Map<ProgramId, RequestInfo> requests = new ConcurrentHashMap<>();

    @Override
    public Future<PreviewRequest> startPreview(PreviewRequest request) {
      CompletableFuture<PreviewRequest> future = new CompletableFuture<>();
      requests.put(request.getProgram(),
          new RequestInfo(
              request,
              future,
              new PreviewStatus(PreviewStatus.Status.RUNNING,
                  System.currentTimeMillis(), null, System.currentTimeMillis(), null),
              SecurityRequestContext.getUserId(),
              SecurityRequestContext.getUserCredential()));
      return future;
    }

    @Override
    public void stopPreview(ProgramId programId) throws Exception {
      RequestInfo info = requests.get(programId);
      if (info == null) {
        throw new NotFoundException(programId);
      }
      info.status = new PreviewStatus(PreviewStatus.Status.KILLED, System.currentTimeMillis(), null,
          info.status.getStartTime(), System.currentTimeMillis());
      info.future.complete(info.request);
    }

    void complete(ProgramId programId) throws NotFoundException {
      RequestInfo info = requests.get(programId);
      if (info == null) {
        throw new NotFoundException(programId);
      }
      info.future.complete(info.request);
    }

    private static final class RequestInfo {
      private final PreviewRequest request;
      private final CompletableFuture<PreviewRequest> future;
      private final String capturedUserId;
      private final Credential capturedCredential;
      private PreviewStatus status;

      private RequestInfo(PreviewRequest request, CompletableFuture<PreviewRequest> future,
          PreviewStatus status, String capturedUserId, Credential capturedCredential) {
        this.request = request;
        this.future = future;
        this.status = status;
        this.capturedUserId = capturedUserId;
        this.capturedCredential = capturedCredential;
      }
    }
  }

  /**
   * A mocking {@link PreviewRequestFetcher} for unit testing.
   */
  private static final class MockPreviewRequestFetcher implements PreviewRequestFetcher {

    private final Queue<PreviewRequest> requests = new ConcurrentLinkedQueue<>();
    private final AtomicInteger fetchCount = new AtomicInteger();

    @Override
    public Optional<PreviewRequest> fetch() {
      fetchCount.incrementAndGet();
      return Optional.ofNullable(requests.poll());
    }

    void addRequest(PreviewRequest request) {
      requests.offer(request);
    }
  }
}
