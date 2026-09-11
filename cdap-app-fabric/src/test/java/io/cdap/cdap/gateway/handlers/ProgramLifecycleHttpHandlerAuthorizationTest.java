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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.cdap.cdap.app.mapreduce.MRJobInfoFetcher;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ProgramRunCluster;
import io.cdap.cdap.proto.ProgramRunClusterStatus;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.DefaultContextAccessEnforcer;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Collections;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Test;

/**
 * Authorization tests for {@link ProgramLifecycleHttpHandler}.
 */
public class ProgramLifecycleHttpHandlerAuthorizationTest {

  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);

  @Test
  public void testProgramRunRecordRequiresProgramGetPermission() throws Exception {
    String namespaceId = NamespaceId.DEFAULT.getNamespace();
    String appName = "app";
    String programName = "worker";
    String runId = "run";
    ProgramReference programRef = new ApplicationReference(namespaceId, appName)
        .program(ProgramType.WORKER, programName);
    Authorizable programAuthorizable = Authorizable.fromEntityId(programRef);
    RunRecordDetail runRecord = createRunRecord(namespaceId, appName, programName, runId);

    Store store = mock(Store.class);
    when(store.getRun(eq(programRef), eq(runId))).thenReturn(runRecord);
    InMemoryAccessController accessController = new InMemoryAccessController();
    accessController.revoke(programAuthorizable);
    ProgramLifecycleHttpHandler handler = createHandler(store,
        new DefaultContextAccessEnforcer(new AuthenticationTestContext(), accessController));
    HttpRequest request = mock(HttpRequest.class);
    HttpResponder responder = mock(HttpResponder.class);

    try {
      AuthenticationTestContext.actAsPrincipal(BOB);
      handler.programRunRecord(request, responder, namespaceId, appName, "workers", programName, runId);
      Assert.fail();
    } catch (UnauthorizedException e) {
      // expected
    }
    verify(store, never()).getRun(any(ProgramReference.class), anyString());

    try {
      accessController.grant(programAuthorizable, ALICE, Collections.singleton(StandardPermission.GET));
      AuthenticationTestContext.actAsPrincipal(ALICE);
      handler.programRunRecord(request, responder, namespaceId, appName, "workers", programName, runId);
    } finally {
      accessController.revoke(programAuthorizable);
    }

    verify(store).getRun(eq(programRef), eq(runId));
    verify(responder).sendJson(eq(HttpResponseStatus.OK), anyString());
  }

  private static ProgramLifecycleHttpHandler createHandler(Store store,
      ContextAccessEnforcer contextAccessEnforcer) {
    return new ProgramLifecycleHttpHandler(
        store,
        mock(DiscoveryServiceClient.class),
        mock(ProgramLifecycleService.class),
        mock(MRJobInfoFetcher.class),
        mock(NamespaceQueryAdmin.class),
        contextAccessEnforcer);
  }

  private static RunRecordDetail createRunRecord(String namespaceId, String appName,
      String programName, String runId) {
    return RunRecordDetail.builder()
        .setProgramRunId(new NamespaceId(namespaceId).app(appName).worker(programName).run(runId))
        .setStartTime(1L)
        .setRunTime(1L)
        .setStatus(ProgramRunStatus.RUNNING)
        .setCluster(new ProgramRunCluster(ProgramRunClusterStatus.PROVISIONED, null, null))
        .setProfileId(ProfileId.NATIVE)
        .setSourceId(new byte[] { 0 })
        .build();
  }
}
