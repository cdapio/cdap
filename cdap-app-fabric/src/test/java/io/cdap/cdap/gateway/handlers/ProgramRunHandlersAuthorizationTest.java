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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.app.mapreduce.MRJobInfoFetcher;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.ApplicationPermission;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Authorization tests for workflow and program run endpoints of {@link WorkflowHttpHandler},
 * {@link WorkflowStatsSLAHttpHandler}, {@link ProgramLifecycleHttpHandler} and
 * {@link ProgramRuntimeHttpHandler}.
 */
public class ProgramRunHandlersAuthorizationTest {

  private static final String NS = "ns";
  private static final String APP = "app";
  private static final String WORKFLOW = "wf";
  private static final String MAPREDUCE = "mr";
  private static final String SERVICE = "svc";
  private static final String RUN = RunIds.generate().getId();

  private static final ApplicationId APP_ID = new ApplicationId(NS, APP);
  private static final ProgramId WORKFLOW_ID = APP_ID.workflow(WORKFLOW);
  private static final ProgramId MAPREDUCE_ID = APP_ID.mr(MAPREDUCE);
  private static final ProgramId SERVICE_ID = APP_ID.service(SERVICE);

  /** Has no permissions. */
  private static final Principal NOBODY = new Principal("wp1-nobody", Principal.PrincipalType.USER);
  /** Has GET on all programs. */
  private static final Principal VIEWER = new Principal("wp1-viewer", Principal.PrincipalType.USER);
  /** Has GET and EXECUTE on the workflow and GET and UPDATE on the service. */
  private static final Principal OPERATOR = new Principal("wp1-operator",
      Principal.PrincipalType.USER);

  private static InMemoryAccessController accessController;
  private static AuthenticationContext authenticationContext;

  private Store store;
  private ProgramRuntimeService runtimeService;
  private HttpRequest request;
  private HttpResponder responder;

  @BeforeClass
  public static void setupClass() {
    accessController = new InMemoryAccessController();
    for (ProgramId programId : new ProgramId[]{WORKFLOW_ID, MAPREDUCE_ID, SERVICE_ID}) {
      grant(programId, VIEWER, StandardPermission.GET);
    }
    grant(WORKFLOW_ID, OPERATOR, StandardPermission.GET, ApplicationPermission.EXECUTE);
    grant(SERVICE_ID, OPERATOR, StandardPermission.GET, StandardPermission.UPDATE);
    authenticationContext = new AuthenticationTestContext();
  }

  @Before
  public void setup() {
    store = mock(Store.class);
    runtimeService = mock(ProgramRuntimeService.class);
    request = mock(HttpRequest.class);
    responder = mock(HttpResponder.class);
  }

  @Test
  public void testWorkflowHttpHandler() throws Exception {
    WorkflowHttpHandler handler = new WorkflowHttpHandler(store, runtimeService,
        mock(DatasetFramework.class), accessController, authenticationContext);
    Call[] getCalls = {
        () -> handler.getWorkflowToken(request, responder, NS, APP, WORKFLOW, RUN, "user", ""),
        () -> handler.getWorkflowToken(request, responder, NS, APP, WORKFLOW, RUN, "node", "user",
            ""),
        () -> handler.getWorkflowNodeStates(request, responder, NS, APP, WORKFLOW, RUN),
        () -> handler.getWorkflowLocalDatasets(request, responder, NS, APP, WORKFLOW, RUN),
    };
    Call[] executeCalls = {
        () -> handler.suspendWorkflowRun(request, responder, NS, APP, WORKFLOW, RUN),
        () -> handler.resumeWorkflowRun(request, responder, NS, APP, WORKFLOW, RUN),
        () -> handler.deleteWorkflowLocalDatasets(request, responder, NS, APP, WORKFLOW, RUN),
    };
    assertAccess(getCalls, NOBODY, VIEWER);
    assertAccess(executeCalls, VIEWER, OPERATOR);
  }

  @Test
  public void testWorkflowStatsSlaHttpHandler() throws Exception {
    WorkflowStatsSLAHttpHandler handler = new WorkflowStatsSLAHttpHandler(store,
        mock(MRJobInfoFetcher.class), mock(MetricsSystemClient.class), accessController,
        authenticationContext);
    Call[] calls = {
        () -> handler.workflowStats(request, responder, NS, APP, WORKFLOW, "now-1d", "now",
            Collections.singletonList(90.0)),
        () -> handler.workflowRunDetail(request, responder, NS, APP, WORKFLOW, RUN, 10, "10s"),
        () -> handler.compare(request, responder, NS, APP, WORKFLOW, RUN, RUN),
    };
    for (Call call : calls) {
      AuthenticationTestContext.actAsPrincipal(NOBODY);
      assertThrows(UnauthorizedException.class, call);
      verifyNoInteractions(store);
    }
    AuthenticationTestContext.actAsPrincipal(VIEWER);
    calls[0].call();
    calls[1].call();
    assertThrows(NotFoundException.class, calls[2]);
  }

  @Test
  public void testProgramLifecycleHttpHandler() throws Exception {
    ProgramLifecycleHttpHandler handler = new ProgramLifecycleHttpHandler(store,
        mock(DiscoveryServiceClient.class), mock(ProgramLifecycleService.class),
        mock(MRJobInfoFetcher.class), mock(NamespaceQueryAdmin.class), accessController,
        authenticationContext);
    Call[] calls = {
        () -> handler.getMapReduceInfo(request, responder, NS, APP, MAPREDUCE, RUN),
        () -> handler.programRunRecord(request, responder, NS, APP, "workflows", WORKFLOW, RUN),
        () -> handler.programRunRecordVersioned(request, responder, NS, APP, "1.0", "workflows",
            WORKFLOW, RUN),
    };
    assertAccess(calls, NOBODY, VIEWER);
  }

  @Test
  public void testProgramRuntimeHttpHandlerLiveInfoAndInstances() throws Exception {
    when(store.getLatestApp(any(ApplicationReference.class))).thenReturn(APP_ID);
    when(store.getApplication(APP_ID)).thenReturn(mock(ApplicationSpecification.class));
    ProgramRuntimeHttpHandler handler = new ProgramRuntimeHttpHandler(
        mock(ProgramLifecycleService.class), store, runtimeService, mock(NamespaceQueryAdmin.class),
        accessController, authenticationContext);

    AuthenticationTestContext.actAsPrincipal(NOBODY);
    assertThrows(UnauthorizedException.class,
        () -> handler.liveInfo(request, responder, NS, APP, "services", SERVICE));
    verifyNoInteractions(runtimeService);
    Assert.assertTrue(getInstances(handler).contains("\"statusCode\":403"));

    AuthenticationTestContext.actAsPrincipal(VIEWER);
    handler.liveInfo(request, responder, NS, APP, "services", SERVICE);
    verify(runtimeService).getLiveInfo(SERVICE_ID);
    // The (mocked) app spec has no such service, so the authorized entry gets a 404 instead
    Assert.assertTrue(getInstances(handler).contains("\"statusCode\":404"));
  }

  @Test
  public void testProgramRuntimeHttpHandlerVersionedLogLevels() throws Exception {
    ProgramRuntimeHttpHandler handler = new ProgramRuntimeHttpHandler(
        mock(ProgramLifecycleService.class), store, runtimeService, mock(NamespaceQueryAdmin.class),
        accessController, authenticationContext);
    AuthenticationTestContext.actAsPrincipal(OPERATOR);

    // The run does not belong to the program in the path
    assertThrows(NotFoundException.class,
        () -> handler.updateProgramLogLevelsVersioned(request("{}"), responder, NS, APP, "1.0",
            "services", SERVICE, RUN));
    assertThrows(NotFoundException.class,
        () -> handler.resetProgramLogLevelsVersioned(request(""), responder, NS, APP, "1.0",
            "services", SERVICE, RUN));
    verifyNoInteractions(runtimeService);

    // The run belongs to the program in the path
    when(store.getRun(eq(SERVICE_ID.getProgramReference()), eq(RUN)))
        .thenReturn(mock(RunRecordDetail.class));
    handler.updateProgramLogLevelsVersioned(request("{}"), responder, NS, APP, "1.0", "services",
        SERVICE, RUN);
    handler.resetProgramLogLevelsVersioned(request(""), responder, NS, APP, "1.0", "services",
        SERVICE, RUN);
    ProgramId versionedServiceId = new ApplicationId(NS, APP, "1.0").service(SERVICE);
    verify(runtimeService).updateProgramLogLevels(eq(versionedServiceId), any(), eq(RUN));
    verify(runtimeService).resetProgramLogLevels(eq(versionedServiceId), any(), eq(RUN));
  }

  private String getInstances(ProgramRuntimeHttpHandler handler) throws Exception {
    HttpResponder responder = mock(HttpResponder.class);
    handler.getInstances(request("[{\"appId\":\"" + APP + "\",\"programType\":\"Service\","
        + "\"programId\":\"" + SERVICE + "\"}]"), responder, NS);
    ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
    verify(responder).sendJson(eq(HttpResponseStatus.OK), json.capture());
    return json.getValue();
  }

  /**
   * Asserts that every call is rejected for {@code denied} without touching the store, and passes
   * authorization for {@code allowed} (the empty mocked store then yields a {@link
   * NotFoundException}).
   */
  private void assertAccess(Call[] calls, Principal denied, Principal allowed) {
    clearInvocations(store);
    for (Call call : calls) {
      AuthenticationTestContext.actAsPrincipal(denied);
      assertThrows(UnauthorizedException.class, call);
      verifyNoInteractions(store);
    }
    for (Call call : calls) {
      AuthenticationTestContext.actAsPrincipal(allowed);
      assertThrows(NotFoundException.class, call);
    }
  }

  private static FullHttpRequest request(String body) {
    return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/",
        Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
  }

  private static void grant(ProgramId programId, Principal principal, Permission... permissions) {
    Set<Permission> permissionSet = ImmutableSet.copyOf(permissions);
    accessController.grant(Authorizable.fromEntityId(programId), principal, permissionSet);
  }

  private static void assertThrows(Class<? extends Exception> expected, Call call) {
    try {
      call.call();
    } catch (Exception e) {
      if (!expected.isInstance(e)) {
        throw new AssertionError("Expected " + expected.getSimpleName() + " but got " + e, e);
      }
      return;
    }
    Assert.fail("Expected " + expected.getSimpleName());
  }

  private interface Call {
    void call() throws Exception;
  }
}
