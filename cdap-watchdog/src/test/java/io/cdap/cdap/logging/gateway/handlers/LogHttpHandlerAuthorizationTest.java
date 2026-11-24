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

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.logging.read.Callback;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.SystemServiceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

/**
 * Tests authorization of the system service log endpoints in {@link LogHttpHandler}.
 */
public class LogHttpHandlerAuthorizationTest {

  private static final Principal PRINCIPAL = new Principal("log-reader", Principal.PrincipalType.USER);

  private LogReader logReader;
  private HttpResponder responder;
  private LogHttpHandler handler;

  @Before
  public void setUp() {
    InMemoryAccessController accessController = new InMemoryAccessController();
    // NamespaceId.SYSTEM GET alone must not allow reading arbitrary system service logs
    accessController.grant(Authorizable.fromEntityId(NamespaceId.SYSTEM),
        PRINCIPAL, Collections.singleton(StandardPermission.GET));
    accessController.grant(Authorizable.fromEntityId(new SystemServiceId("allowed")),
        PRINCIPAL, Collections.singleton(StandardPermission.GET));
    AuthenticationTestContext.actAsPrincipal(PRINCIPAL);

    logReader = Mockito.mock(LogReader.class);
    // the log callbacks write to the chunk responder returned by the HttpResponder
    responder = Mockito.mock(HttpResponder.class, Mockito.RETURNS_MOCKS);
    Answer<Void> initCallback = invocation -> {
      invocation.<Callback>getArgument(4).init();
      return null;
    };
    Mockito.doAnswer(initCallback).when(logReader).getLogNext(Mockito.any(), Mockito.any(),
        Mockito.anyInt(), Mockito.any(), Mockito.any());
    Mockito.doAnswer(initCallback).when(logReader).getLogPrev(Mockito.any(), Mockito.any(),
        Mockito.anyInt(), Mockito.any(), Mockito.any());
    handler = new LogHttpHandler(accessController, new AuthenticationTestContext(),
        logReader, null, CConfiguration.create());
  }

  @Test
  public void testSystemServiceLogs() throws Exception {
    handler.sysList(null, responder, "services", "allowed", -1, -1, true, "", "text",
        Collections.emptyList());
    handler.sysNext(null, responder, "services", "allowed", 50, "", true, "", "text",
        Collections.emptyList());
    handler.sysPrev(null, responder, "services", "allowed", 50, "", true, "", "text",
        Collections.emptyList());
    Mockito.verify(logReader).getLog(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(),
        Mockito.any());
    Mockito.verify(logReader).getLogNext(Mockito.any(), Mockito.any(), Mockito.anyInt(),
        Mockito.any(), Mockito.any());
    Mockito.verify(logReader).getLogPrev(Mockito.any(), Mockito.any(), Mockito.anyInt(),
        Mockito.any(), Mockito.any());

    assertUnauthorized(() -> handler.sysList(null, responder, "services", "denied", -1, -1, true,
        "", "text", Collections.emptyList()));
    assertUnauthorized(() -> handler.sysNext(null, responder, "services", "denied", 50, "", true,
        "", "text", Collections.emptyList()));
    assertUnauthorized(() -> handler.sysPrev(null, responder, "services", "denied", 50, "", true,
        "", "text", Collections.emptyList()));
    Mockito.verifyNoMoreInteractions(logReader);
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
