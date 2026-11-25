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

package io.cdap.cdap.internal.tethering;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.program.MessagingProgramStatePublisher;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.InstancePermission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.DefaultContextAccessEnforcer;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Verifies authorization checks on {@link TetheringHandler} and {@link TetheringServerHandler}.
 */
public class TetheringAuthorizationTest {

  private static final Principal ACCESSOR = new Principal("accessor", Principal.PrincipalType.USER);
  private static final Principal TETHER_AGENT =
      new Principal("tether-agent", Principal.PrincipalType.USER);
  private static final Principal ADMIN = new Principal("admin", Principal.PrincipalType.USER);

  private TetheringStore store;
  private HttpResponder responder;
  private TetheringHandler tetheringHandler;
  private TetheringServerHandler serverHandler;

  @Before
  public void setUp() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Tethering.TETHERING_SERVER_ENABLED, true);

    InMemoryAccessController accessController = new InMemoryAccessController();
    accessController.grant(Authorizable.fromEntityId(InstanceId.SELF), ACCESSOR,
        Collections.singleton(StandardPermission.GET));
    accessController.grant(Authorizable.fromEntityId(InstanceId.SELF), TETHER_AGENT,
        Collections.singleton(InstancePermission.TETHER));
    accessController.grant(Authorizable.fromEntityId(InstanceId.SELF), ADMIN,
        Collections.singleton(StandardPermission.UPDATE));

    ContextAccessEnforcer enforcer = new DefaultContextAccessEnforcer(
        new AuthenticationTestContext(), accessController);
    store = Mockito.mock(TetheringStore.class);
    responder = Mockito.mock(HttpResponder.class);
    MessagingService messagingService = Mockito.mock(MessagingService.class);
    tetheringHandler = new TetheringHandler(cConf, store, messagingService,
        Mockito.mock(ProfileService.class), enforcer);
    serverHandler = new TetheringServerHandler(cConf, store, messagingService, enforcer,
        Mockito.mock(MessagingProgramStatePublisher.class));
  }

  @Test
  public void testTetheringHandlerRequiresInstanceUpdate() throws Exception {
    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    Assert.assertThrows(UnauthorizedException.class,
        () -> tetheringHandler.getTethers(null, responder));
    Assert.assertThrows(UnauthorizedException.class,
        () -> tetheringHandler.getTether(null, responder, "peer1"));
    Assert.assertThrows(UnauthorizedException.class,
        () -> tetheringHandler.deleteTether(null, responder, "peer1"));
    Mockito.verifyNoInteractions(store);

    AuthenticationTestContext.actAsPrincipal(ADMIN);
    Mockito.when(store.getPeers()).thenReturn(Collections.emptyList());
    tetheringHandler.getTethers(null, responder);
    Mockito.verify(store).getPeers();
  }

  @Test
  public void testTetheringServerHandlerChannelsAndActions() throws Exception {
    FullHttpRequest actionReq = jsonPost("{\"action\":\"reject\"}");
    FullHttpRequest channelReq = jsonPost("{}");

    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    Assert.assertThrows(UnauthorizedException.class,
        () -> serverHandler.tetheringAction(actionReq, responder, "peer1"));
    Assert.assertThrows(UnauthorizedException.class,
        () -> serverHandler.pollControlChannel(channelReq, responder, "peer1"));
    Assert.assertThrows(UnauthorizedException.class,
        () -> serverHandler.connectControlChannel(channelReq, responder, "peer1"));
    Mockito.verifyNoInteractions(store);

    // Tethering agent can poll control channels, but cannot accept/reject tether requests.
    AuthenticationTestContext.actAsPrincipal(TETHER_AGENT);
    Assert.assertThrows(UnauthorizedException.class,
        () -> serverHandler.tetheringAction(actionReq, responder, "peer1"));
    Mockito.when(store.getPeer("peer1")).thenThrow(new PeerNotFoundException("peer1"));
    serverHandler.pollControlChannel(channelReq, responder, "peer1");
    Mockito.verify(store).getPeer("peer1");

    // Admin can accept/reject tether requests.
    AuthenticationTestContext.actAsPrincipal(ADMIN);
    serverHandler.tetheringAction(actionReq, responder, "peer1");
    Mockito.verify(store, Mockito.times(2)).getPeer("peer1");
  }

  private static FullHttpRequest jsonPost(String body) {
    return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/v3/tethering",
        Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
  }
}
