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

package io.cdap.cdap.metadata;

import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Authorization tests for {@link LineageHTTPHandler} endpoints that are not dataset scoped.
 */
public class LineageHttpHandlerAuthorizationTest {

  private static final Principal ALICE = new Principal("lineage-alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("lineage-bob", Principal.PrincipalType.USER);

  @Test
  public void testGetEndpointSummary() throws Exception {
    ProgramId program = new NamespaceId("lineagens").app("app").workflow("wf");
    InMemoryAccessController accessController = new InMemoryAccessController();
    accessController.grant(Authorizable.fromEntityId(program), ALICE,
        Collections.singleton(StandardPermission.GET));
    FieldLineageAdmin fieldLineageAdmin = Mockito.mock(FieldLineageAdmin.class);
    LineageHTTPHandler handler = new LineageHTTPHandler(Mockito.mock(LineageAdmin.class),
        fieldLineageAdmin, accessController, new AuthenticationTestContext());
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    String runId = RunIds.generate().getId();

    AuthenticationTestContext.actAsPrincipal(BOB);
    try {
      handler.getEndpointSummary(null, responder, program.getNamespace(), program.getApplication(),
          ProgramType.WORKFLOW.getCategoryName(), program.getProgram(), runId);
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
    Mockito.verifyNoInteractions(fieldLineageAdmin);

    AuthenticationTestContext.actAsPrincipal(ALICE);
    handler.getEndpointSummary(null, responder, program.getNamespace(), program.getApplication(),
        ProgramType.WORKFLOW.getCategoryName(), program.getProgram(), runId);
    Mockito.verify(fieldLineageAdmin).getEndpoints(Mockito.eq(program.getNamespace()),
        Mockito.eq(program.getProgramReference()), Mockito.any());
  }
}
