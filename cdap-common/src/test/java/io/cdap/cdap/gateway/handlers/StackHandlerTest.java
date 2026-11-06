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

import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.security.InstancePermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link StackHandler}.
 */
public class StackHandlerTest {

  @Test
  public void testStacksRequiresHealthCheck() {
    ContextAccessEnforcer enforcer = Mockito.mock(ContextAccessEnforcer.class);
    StackHandler handler = new StackHandler(enforcer);
    HttpResponder responder = Mockito.mock(HttpResponder.class);

    Mockito.doThrow(new UnauthorizedException("denied"))
        .when(enforcer).enforce(InstanceId.SELF, InstancePermission.HEALTH_CHECK);
    try {
      handler.stacks(null, responder, "test", 1);
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
    Mockito.verifyNoInteractions(responder);

    Mockito.reset(enforcer);
    handler.stacks(null, responder, "test", 1);
    Mockito.verify(enforcer).enforce(InstanceId.SELF, InstancePermission.HEALTH_CHECK);
    Mockito.verify(responder).sendString(Mockito.eq(HttpResponseStatus.OK),
        Mockito.contains("Process Thread Dump: test"));
  }
}
