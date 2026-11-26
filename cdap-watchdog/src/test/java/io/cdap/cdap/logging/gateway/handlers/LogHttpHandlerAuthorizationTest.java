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

package io.cdap.cdap.logging.gateway.handlers;

import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.logging.ErrorLogsClassifier;
import io.cdap.cdap.logging.read.Callback;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.SystemServiceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.authorization.InMemoryPrivilegeHolder;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import java.util.Collections;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

/**
 * Tests authorization of system service and system namespace program log endpoints in
 * {@link LogHttpHandler} and {@link ErrorClassificationHttpHandler}.
 */
public class LogHttpHandlerAuthorizationTest {

  private static final Principal ACCESSOR = new Principal("accessor", Principal.PrincipalType.USER);
  private static final Principal ADMIN = new Principal("admin", Principal.PrincipalType.USER);

  private LogReader logReader;
  private ErrorLogsClassifier classifier;
  private HttpResponder responder;
  private LogHttpHandler handler;
  private ErrorClassificationHttpHandler classifyHandler;

  @Before
  public void setUp() throws Exception {
    InMemoryPrivilegeHolder.getPrivileges().clear();
    InMemoryPrivilegeHolder.getRoleToPrincipals().clear();
    InMemoryAccessController accessController = new InMemoryAccessController();
    // Granting GET on InstanceId.SELF, SystemServiceId, NamespaceId.SYSTEM, and system programs
    // simulates CDF's Accessor + system-namespace read bypass; system logs must still require
    // InstanceId.SELF UPDATE.
    ProgramId systemStudio = new ProgramId(
        new ApplicationId("system", "pipeline"), ProgramType.SERVICE, "studio");
    ProgramId userProgram = new ProgramId(
        new ApplicationId("userns", "app1"), ProgramType.SERVICE, "svc");

    accessController.grant(Authorizable.fromEntityId(InstanceId.SELF),
        ACCESSOR, Collections.singleton(StandardPermission.GET));
    accessController.grant(Authorizable.fromEntityId(NamespaceId.SYSTEM),
        ACCESSOR, Collections.singleton(StandardPermission.GET));
    accessController.grant(Authorizable.fromEntityId(new SystemServiceId("appfabric")),
        ACCESSOR, Collections.singleton(StandardPermission.GET));
    accessController.grant(Authorizable.fromEntityId(systemStudio),
        ACCESSOR, Collections.singleton(StandardPermission.GET));
    accessController.grant(Authorizable.fromEntityId(userProgram),
        ACCESSOR, Collections.singleton(StandardPermission.GET));

    accessController.grant(Authorizable.fromEntityId(InstanceId.SELF),
        ADMIN, Collections.singleton(StandardPermission.UPDATE));

    logReader = Mockito.mock(LogReader.class);
    Mockito.when(logReader.getLog(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(),
        Mockito.any())).thenReturn(CloseableIterator.empty());
    ProgramRunRecordFetcher runRecordFetcher = Mockito.mock(ProgramRunRecordFetcher.class);
    RunRecordDetail failedRun = RunRecordDetail.builder()
        .setProgramRunId(systemStudio.run("run-1"))
        .setStartTime(1L)
        .setStatus(ProgramRunStatus.FAILED)
        .setSourceId(new byte[]{1})
        .build();
    Mockito.when(runRecordFetcher.getRunRecordMeta(Mockito.any(), Mockito.eq("run-1")))
        .thenReturn(failedRun);

    responder = Mockito.mock(HttpResponder.class, Mockito.RETURNS_MOCKS);
    Answer<Void> initCallback = invocation -> {
      invocation.<Callback>getArgument(4).init();
      return null;
    };
    Mockito.doAnswer(initCallback).when(logReader).getLogNext(Mockito.any(), Mockito.any(),
        Mockito.anyInt(), Mockito.any(), Mockito.any());
    Mockito.doAnswer(initCallback).when(logReader).getLogPrev(Mockito.any(), Mockito.any(),
        Mockito.anyInt(), Mockito.any(), Mockito.any());

    CConfiguration cConf = CConfiguration.create();
    AuthenticationTestContext authCtx = new AuthenticationTestContext();
    classifier = Mockito.mock(ErrorLogsClassifier.class);
    handler = new LogHttpHandler(accessController, authCtx, logReader, runRecordFetcher, cConf);
    classifyHandler = new ErrorClassificationHttpHandler(accessController, authCtx, logReader,
        runRecordFetcher, classifier, cConf);
  }

  @After
  public void tearDown() {
    SecurityRequestContext.reset();
    InMemoryPrivilegeHolder.getPrivileges().clear();
    InMemoryPrivilegeHolder.getRoleToPrincipals().clear();
  }

  @Test
  public void testSystemServiceLogsRequireInstanceUpdate() throws Exception {
    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    assertUnauthorized(() -> handler.sysList(null, responder, "services", "appfabric", -1, -1,
        true, "", "text", Collections.emptyList()));
    assertUnauthorized(() -> handler.sysNext(null, responder, "services", "appfabric", 50, "",
        true, "", "text", Collections.emptyList()));
    assertUnauthorized(() -> handler.sysPrev(null, responder, "services", "appfabric", 50, "",
        true, "", "text", Collections.emptyList()));
    Mockito.verifyNoInteractions(logReader);

    AuthenticationTestContext.actAsPrincipal(ADMIN);
    handler.sysList(null, responder, "services", "appfabric", -1, -1, true, "", "text",
        Collections.emptyList());
    handler.sysNext(null, responder, "services", "appfabric", 50, "", true, "", "text",
        Collections.emptyList());
    handler.sysPrev(null, responder, "services", "appfabric", 50, "", true, "", "text",
        Collections.emptyList());
    Mockito.verify(logReader).getLog(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(),
        Mockito.any());
    Mockito.verify(logReader).getLogNext(Mockito.any(), Mockito.any(), Mockito.eq(50),
        Mockito.any(), Mockito.any());
    Mockito.verify(logReader).getLogPrev(Mockito.any(), Mockito.any(), Mockito.eq(50),
        Mockito.any(), Mockito.any());
  }

  @Test
  public void testSystemNamespaceProgramLogsRequireInstanceUpdate() throws Exception {
    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    assertUnauthorized(() -> handler.getLogs(null, responder, "system", "pipeline", "services",
        "studio", -1, -1, true, "", "text", Collections.emptyList()));
    assertUnauthorized(() -> handler.getLogs(null, responder, "SYSTEM", "pipeline", "services",
        "studio", -1, -1, true, "", "text", Collections.emptyList()));
    assertUnauthorized(() -> handler.getRunIdLogs(null, responder, "system", "pipeline",
        "services", "studio", "run-1", -1, -1, true, "", "text", Collections.emptyList()));
    assertUnauthorized(() -> handler.next(null, responder, "system", "pipeline", "services",
        "studio", 50, "", true, "", "text", Collections.emptyList()));
    assertUnauthorized(() -> handler.runIdNext(null, responder, "system", "pipeline", "services",
        "studio", "run-1", 50, "", true, "", "text", Collections.emptyList()));
    assertUnauthorized(() -> handler.prev(null, responder, "system", "pipeline", "services",
        "studio", 50, "", true, "", "text", Collections.emptyList()));
    assertUnauthorized(() -> handler.runIdPrev(null, responder, "system", "pipeline", "services",
        "studio", "run-1", 50, "", true, "", "text", Collections.emptyList()));
    assertUnauthorized(() -> classifyHandler.classifyRunIdLogs(null, responder, "system",
        "pipeline", "services", "studio", "run-1"));
    assertUnauthorized(() -> classifyHandler.classifyRunIdLogs(null, responder, "SYSTEM",
        "pipeline", "services", "studio", "run-1"));
    Mockito.verifyNoInteractions(logReader);

    // User-namespace program logs and error classification still succeed with ProgramId GET.
    handler.getLogs(null, responder, "userns", "app1", "services", "svc", -1, -1, true, "",
        "text", Collections.emptyList());
    handler.next(null, responder, "userns", "app1", "services", "svc", 50, "", true, "", "text",
        Collections.emptyList());
    handler.runIdNext(null, responder, "userns", "app1", "services", "svc", "run-1", 50, "", true,
        "", "text", Collections.emptyList());
    handler.prev(null, responder, "userns", "app1", "services", "svc", 50, "", true, "", "text",
        Collections.emptyList());
    handler.runIdPrev(null, responder, "userns", "app1", "services", "svc", "run-1", 50, "", true,
        "", "text", Collections.emptyList());
    classifyHandler.classifyRunIdLogs(null, responder, "userns", "app1", "services", "svc",
        "run-1");
    Mockito.verify(classifier).classify(Mockito.any(), Mockito.eq(responder),
        Mockito.eq("userns"), Mockito.eq("svc"), Mockito.eq("app1"), Mockito.eq("run-1"));

    // Admin with InstanceId.SELF UPDATE can read and classify system program logs.
    AuthenticationTestContext.actAsPrincipal(ADMIN);
    handler.getLogs(null, responder, "system", "pipeline", "services", "studio", -1, -1, true,
        "", "text", Collections.emptyList());
    handler.getRunIdLogs(null, responder, "system", "pipeline", "services", "studio", "run-1", -1,
        -1, true, "", "text", Collections.emptyList());
    handler.next(null, responder, "system", "pipeline", "services", "studio", 50, "", true, "",
        "text", Collections.emptyList());
    handler.runIdNext(null, responder, "system", "pipeline", "services", "studio", "run-1", 50,
        "", true, "", "text", Collections.emptyList());
    handler.prev(null, responder, "system", "pipeline", "services", "studio", 50, "", true, "",
        "text", Collections.emptyList());
    handler.runIdPrev(null, responder, "system", "pipeline", "services", "studio", "run-1", 50,
        "", true, "", "text", Collections.emptyList());
    classifyHandler.classifyRunIdLogs(null, responder, "system", "pipeline", "services", "studio",
        "run-1");
    Mockito.verify(classifier).classify(Mockito.any(), Mockito.eq(responder),
        Mockito.eq("system"), Mockito.eq("studio"), Mockito.eq("pipeline"), Mockito.eq("run-1"));
  }

  private static void assertUnauthorized(ThrowingRunnable runnable) throws Exception {
    try {
      runnable.run();
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
