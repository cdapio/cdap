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

package io.cdap.cdap.gateway.handlers;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.data2.registry.UsageRegistry;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.runtime.schedule.TimeSchedulerService;
import io.cdap.cdap.internal.bootstrap.BootstrapService;
import io.cdap.cdap.internal.operation.OperationLifecycleManager;
import io.cdap.cdap.internal.operation.OperationRunDetail;
import io.cdap.cdap.internal.operation.OperationStatePublisher;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.operation.OperationMeta;
import io.cdap.cdap.proto.operation.OperationRun;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import io.cdap.cdap.proto.operation.OperationType;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.reporting.ProgramHeartbeatService;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.DefaultContextAccessEnforcer;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.ChunkResponder;
import io.cdap.http.HttpResponder;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionSystemClient;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Verifies that public /v3 endpoints of various app-fabric handlers enforce authorization before
 * reading or mutating any state.
 */
public class PublicEndpointsAuthorizationTest {

  private static final Principal NOBODY = new Principal("pea-nobody", Principal.PrincipalType.USER);
  private static final Principal VIEWER = new Principal("pea-viewer", Principal.PrincipalType.USER);
  private static final Principal EDITOR = new Principal("pea-editor", Principal.PrincipalType.USER);

  private static final NamespaceId NAMESPACE = new NamespaceId("peans");
  private static final ApplicationId APP = NAMESPACE.app("app");
  private static final ProgramId PROGRAM = APP.workflow("wf");
  private static final DatasetId DATASET = NAMESPACE.dataset("ds");

  private static InMemoryAccessController accessController;
  private static AuthenticationContext authenticationContext;

  @BeforeClass
  public static void setup() {
    accessController = new InMemoryAccessController();
    authenticationContext = new AuthenticationTestContext();
    for (EntityId entity : ImmutableSet.of(InstanceId.SELF, NAMESPACE, APP, PROGRAM, DATASET)) {
      accessController.grant(Authorizable.fromEntityId(entity), VIEWER,
          EnumSet.of(StandardPermission.GET));
      accessController.grant(Authorizable.fromEntityId(entity), EDITOR,
          EnumSet.of(StandardPermission.GET, StandardPermission.UPDATE));
    }
  }

  @Test
  public void testTransactionHandler() throws Exception {
    TransactionSystemClient txClient = Mockito.mock(TransactionSystemClient.class);
    Mockito.when(txClient.startShort())
        .thenReturn(new Transaction(1L, 2L, new long[0], new long[0], 1L));
    TransactionHttpHandler handler = new TransactionHttpHandler(new Configuration(),
        CConfiguration.create(), txClient, accessController, authenticationContext);
    HttpResponder responder = Mockito.mock(HttpResponder.class);

    // read endpoints require GET on the instance
    assertOnlyAllowed(VIEWER, NOBODY, txClient,
        () -> handler.getTxManagerSnapshot(null, responder));
    assertOnlyAllowed(VIEWER, NOBODY, txClient, () -> handler.invalidTxSize(null, responder));
    assertOnlyAllowed(VIEWER, NOBODY, txClient, () -> handler.invalidList(null, responder, -1));
    assertOnlyAllowed(VIEWER, NOBODY, txClient,
        () -> handler.getPruneInfo(null, responder, "region"));
    assertOnlyAllowed(VIEWER, NOBODY, txClient,
        () -> handler.getTimeRegions(null, responder, "now"));
    assertOnlyAllowed(VIEWER, NOBODY, txClient,
        () -> handler.getIdleRegions(null, responder, -1, "now"));
    assertOnlyAllowed(VIEWER, NOBODY, txClient,
        () -> handler.getRegionsToBeCompacted(null, responder, -1, "now"));

    // mutating endpoints require UPDATE on the instance
    assertOnlyAllowed(EDITOR, VIEWER, txClient, () -> handler.invalidateTx(null, responder, "1"));
    assertOnlyAllowed(EDITOR, VIEWER, txClient,
        () -> handler.truncateInvalidTxBefore(jsonRequest("{\"time\": 1}"), responder));
    assertOnlyAllowed(EDITOR, VIEWER, txClient,
        () -> handler.truncateInvalidTx(jsonRequest("{\"ids\": [1]}"), responder));
    assertOnlyAllowed(EDITOR, VIEWER, txClient,
        () -> handler.resetTxManagerState(null, responder));
    Mockito.verify(txClient).resetState();
    assertOnlyAllowed(EDITOR, VIEWER, txClient, () -> handler.pruneNow(null, responder));
    Mockito.verify(txClient).pruneNow();
  }

  @Test
  public void testOperationalStatsHandler() throws Exception {
    OperationalStatsHttpHandler handler = new OperationalStatsHttpHandler(
        new DefaultContextAccessEnforcer(authenticationContext, accessController));
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    assertOnlyAllowed(VIEWER, NOBODY, responder, () -> handler.getServiceProviders(null, responder));
    assertOnlyAllowed(VIEWER, NOBODY, responder, () -> {
      try {
        handler.getServiceProviderStats(null, responder, "unknown");
        Assert.fail("Expected NotFoundException for an unknown service provider");
      } catch (NotFoundException e) {
        // expected once the authorization check passed
      }
    });
  }

  @Test
  public void testUsageHandler() throws Exception {
    UsageRegistry registry = Mockito.mock(UsageRegistry.class);
    UsageHandler handler = new UsageHandler(registry, accessController, authenticationContext);
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    assertOnlyAllowed(VIEWER, NOBODY, registry, () -> handler.getAppDatasetUsage(null, responder,
        APP.getNamespace(), APP.getApplication()));
    Mockito.verify(registry).getDatasets(APP);
    assertOnlyAllowed(VIEWER, NOBODY, registry, () -> handler.getProgramDatasetUsage(null,
        responder, PROGRAM.getNamespace(), PROGRAM.getApplication(),
        PROGRAM.getType().getCategoryName(), PROGRAM.getProgram()));
    Mockito.verify(registry).getDatasets(PROGRAM);
    assertOnlyAllowed(VIEWER, NOBODY, registry, () -> handler.getDatasetAppUsage(null, responder,
        DATASET.getNamespace(), DATASET.getDataset()));
    Mockito.verify(registry).getPrograms(DATASET);
  }

  @Test
  public void testBootstrapHandler() throws Exception {
    BootstrapService bootstrapService = Mockito.mock(BootstrapService.class);
    BootstrapHttpHandler handler = new BootstrapHttpHandler(bootstrapService, accessController,
        authenticationContext);
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    assertOnlyAllowed(EDITOR, VIEWER, bootstrapService, () -> handler.bootstrap(null, responder));
    Mockito.verify(bootstrapService).reload();
    Mockito.verify(bootstrapService).bootstrap();
  }

  @Test
  public void testOperationsDashboardHandler() throws Exception {
    ProgramHeartbeatService heartbeatService = Mockito.mock(ProgramHeartbeatService.class);
    OperationsDashboardHttpHandler handler = new OperationsDashboardHttpHandler(heartbeatService,
        Mockito.mock(Scheduler.class), Mockito.mock(TimeSchedulerService.class), accessController,
        authenticationContext);
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    assertOnlyAllowed(VIEWER, NOBODY, heartbeatService, () -> handler.readDashboardDetail(null,
        responder, 0, 0, Collections.singleton(NAMESPACE.getNamespace())));
    Mockito.verify(heartbeatService).scan(Mockito.anyLong(), Mockito.anyLong(), Mockito.any());
    // all requested namespaces must be accessible
    AuthenticationTestContext.actAsPrincipal(VIEWER);
    assertUnauthorized(() -> handler.readDashboardDetail(null, responder, 0, 0,
        ImmutableSet.of(NAMESPACE.getNamespace(), "otherns")));
  }

  @Test
  public void testOperationHandler() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(
        "feature." + Feature.SOURCE_CONTROL_MANAGEMENT_MULTI_APP.getFeatureFlagString(), true);
    OperationLifecycleManager manager = Mockito.mock(OperationLifecycleManager.class);
    OperationRunId runId = new OperationRunId(NAMESPACE.getNamespace(), "run");
    OperationRunDetail detail = Mockito.mock(OperationRunDetail.class);
    Mockito.when(detail.getRun()).thenReturn(OperationRun.builder()
        .setRunId(runId.getRun())
        .setStatus(OperationRunStatus.RUNNING)
        .setType(OperationType.PUSH_APPS)
        .setMetadata(OperationMeta.builder().setCreateTime(Instant.now()).build())
        .build());
    Mockito.when(manager.getOperationRun(runId)).thenReturn(detail);
    OperationHttpHandler handler = new OperationHttpHandler(cConf, manager,
        Mockito.mock(OperationStatePublisher.class), accessController, authenticationContext);
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    Mockito.when(responder.sendChunkStart(HttpResponseStatus.OK))
        .thenReturn(Mockito.mock(ChunkResponder.class));

    assertOnlyAllowed(VIEWER, NOBODY, manager, () -> handler.scanOperations(null, responder,
        NAMESPACE.getNamespace(), null, null, null));
    Mockito.verify(manager).scanOperations(Mockito.any(), Mockito.anyInt(), Mockito.any());
    assertOnlyAllowed(VIEWER, NOBODY, manager, () -> handler.getOperationRun(null, responder,
        NAMESPACE.getNamespace(), runId.getRun()));
    Mockito.verify(manager).getOperationRun(runId);
    assertOnlyAllowed(EDITOR, VIEWER, manager, () -> handler.failOperation(null, responder,
        NAMESPACE.getNamespace(), runId.getRun()));
    Mockito.verify(manager).sendStopNotification(runId);
  }

  /**
   * Asserts that the given call is rejected for {@code denied} without touching {@code mock}, and
   * that it succeeds for {@code allowed}.
   */
  private static void assertOnlyAllowed(Principal allowed, Principal denied, Object mock,
      ThrowingRunnable call) throws Exception {
    Mockito.clearInvocations(mock);
    AuthenticationTestContext.actAsPrincipal(denied);
    assertUnauthorized(call);
    Mockito.verifyNoInteractions(mock);
    AuthenticationTestContext.actAsPrincipal(allowed);
    call.run();
  }

  private static void assertUnauthorized(ThrowingRunnable call) throws Exception {
    try {
      call.run();
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
  }

  private static FullHttpRequest jsonRequest(String body) {
    return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/",
        Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
  }

  /**
   * A runnable that can throw checked exceptions.
   */
  private interface ThrowingRunnable {

    void run() throws Exception;
  }
}
