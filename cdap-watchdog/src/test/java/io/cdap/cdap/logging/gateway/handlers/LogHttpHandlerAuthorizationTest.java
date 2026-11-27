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

import static org.mockito.Mockito.mock;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import java.util.Collections;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that logs of the system namespace need instance-level read access, while logs of user
 * namespaces keep the per-program check only.
 */
public class LogHttpHandlerAuthorizationTest {

  // Can read system namespace entities (as authorizers that allow all system reads do), but
  // holds no instance-level read access.
  private static final Principal ACCESSOR = new Principal("log-accessor",
      Principal.PrincipalType.USER);
  // Instance-level reader.
  private static final Principal READER = new Principal("log-reader",
      Principal.PrincipalType.USER);
  // Single-namespace user: GET on a program in ns1 only.
  private static final Principal NS_USER = new Principal("log-ns-user",
      Principal.PrincipalType.USER);

  private static final ProgramId STUDIO = NamespaceId.SYSTEM.app("pipeline")
      .program(ProgramType.SERVICE, "studio");
  private static final ProgramId NS1_PIPELINE = new NamespaceId("ns1").app("p1")
      .program(ProgramType.WORKFLOW, "DataPipelineWorkflow");

  private static LogHttpHandler handler;

  @BeforeClass
  public static void setup() {
    InMemoryAccessController accessController = new InMemoryAccessController();
    for (Principal principal : new Principal[] {ACCESSOR, READER}) {
      accessController.grant(Authorizable.fromEntityId(NamespaceId.SYSTEM), principal,
          Collections.singleton(StandardPermission.GET));
      accessController.grant(Authorizable.fromEntityId(STUDIO), principal,
          Collections.singleton(StandardPermission.GET));
    }
    accessController.grant(Authorizable.fromEntityId(InstanceId.SELF, EntityType.NAMESPACE),
        READER, Collections.singleton(StandardPermission.LIST));
    accessController.grant(Authorizable.fromEntityId(NS1_PIPELINE), NS_USER,
        Collections.singleton(StandardPermission.GET));

    handler = new LogHttpHandler(accessController, new AuthenticationTestContext(),
        mock(LogReader.class), mock(ProgramRunRecordFetcher.class), CConfiguration.create());
  }

  @Test
  public void testSystemServiceLogsNeedInstanceRead() throws Exception {
    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    try {
      getSystemServiceLogs();
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException expected) {
      // expected
    }
    AuthenticationTestContext.actAsPrincipal(READER);
    getSystemServiceLogs();
  }

  @Test
  public void testSystemAppLogsNeedInstanceRead() throws Exception {
    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    try {
      getProgramLogs(STUDIO);
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException expected) {
      // expected
    }
    AuthenticationTestContext.actAsPrincipal(READER);
    getProgramLogs(STUDIO);
  }

  @Test
  public void testUserNamespaceLogsUnchanged() throws Exception {
    AuthenticationTestContext.actAsPrincipal(NS_USER);
    getProgramLogs(NS1_PIPELINE);
  }

  private static void getSystemServiceLogs() throws Exception {
    handler.sysList(mock(HttpRequest.class), mock(HttpResponder.class), "services", "appfabric",
        -1, -1, true, "", "text", Collections.emptyList());
  }

  private static void getProgramLogs(ProgramId program) throws Exception {
    handler.getLogs(mock(HttpRequest.class), mock(HttpResponder.class), program.getNamespace(),
        program.getApplication(), program.getType().getCategoryName(), program.getProgram(),
        -1, -1, true, "", "text", Collections.emptyList());
  }
}
