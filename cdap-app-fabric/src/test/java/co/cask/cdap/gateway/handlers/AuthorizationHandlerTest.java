/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.security.authorization.AuthorizationPlugin;
import co.cask.cdap.security.authorization.InMemoryAuthorizationPlugin;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 *
 */
public class AuthorizationHandlerTest {

  private InMemoryAuthorizationPlugin auth;
  private NettyHttpService service;
  private AuthorizationClient client;

  @Before
  public void setUp() {
    CConfiguration conf = CConfiguration.create();
    conf.setBoolean(Constants.Security.Authorization.ENABLED, true);

    auth = new InMemoryAuthorizationPlugin();
    service = NettyHttpService.builder()
      .addHttpHandlers(ImmutableList.of(new AuthorizationHandler(auth, conf)))
      .build();
    service.startAndWait();

    client = new AuthorizationClient(
      ClientConfig.builder()
        .setConnectionConfig(
          ConnectionConfig.builder()
            .setHostname(service.getBindAddress().getHostName())
            .setPort(service.getBindAddress().getPort())
            .setSSLEnabled(false)
            .build())
        .build());
  }

  @After
  public void tearDown() {
    service.stopAndWait();
  }

  @Test
  public void testDisabled() throws IOException, UnauthorizedException {
    CConfiguration conf = CConfiguration.create();
    conf.setBoolean(Constants.Security.Authorization.ENABLED, false);

    AuthorizationPlugin auth = new InMemoryAuthorizationPlugin();
    NettyHttpService service = NettyHttpService.builder()
      .addHttpHandlers(ImmutableList.of(new AuthorizationHandler(auth, conf)))
      .build();
    service.startAndWait();

    AuthorizationClient client = new AuthorizationClient(
      ClientConfig.builder()
        .setConnectionConfig(
          ConnectionConfig.builder()
            .setHostname(service.getBindAddress().getHostName())
            .setPort(service.getBindAddress().getPort())
            .setSSLEnabled(false)
            .build())
        .build());

    NamespaceId ns1 = Ids.namespace("ns1");
    try {
      client.authorized(ns1, "admin", ImmutableSet.of(Action.READ));
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("404"));
    }

    try {
      client.grant(ns1, "admin", ImmutableSet.of(Action.READ));
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("404"));
    }

    try {
      client.revoke(ns1, "admin", ImmutableSet.of(Action.READ));
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("404"));
    }
  }

  @Test
  public void testRevokeEntityUserActions() throws IOException, UnauthorizedException {
    NamespaceId ns1 = Ids.namespace("ns1");

    // grant() and revoke(EntityId, String, Set<Action>)
    Assert.assertEquals(false, client.authorized(ns1, "admin", ImmutableSet.of(Action.READ)));
    client.grant(ns1, "admin", ImmutableSet.of(Action.READ));
    Assert.assertEquals(true, client.authorized(ns1, "admin", ImmutableSet.of(Action.READ)));
    client.revoke(ns1, "admin", ImmutableSet.of(Action.READ));
    Assert.assertEquals(false, client.authorized(ns1, "admin", ImmutableSet.of(Action.READ)));
  }

  @Test
  public void testRevokeEntityUser() throws IOException, UnauthorizedException {
    NamespaceId ns1 = Ids.namespace("ns1");

    // grant() and revoke(EntityId, String)
    client.grant(ns1, "admin", ImmutableSet.of(Action.READ));
    client.grant(ns1, "bob", ImmutableSet.of(Action.READ));
    Assert.assertEquals(true, client.authorized(ns1, "admin", ImmutableSet.of(Action.READ)));
    Assert.assertEquals(true, client.authorized(ns1, "bob", ImmutableSet.of(Action.READ)));
    client.revoke(ns1, "admin");
    Assert.assertEquals(false, client.authorized(ns1, "admin", ImmutableSet.of(Action.READ)));
    Assert.assertEquals(true, client.authorized(ns1, "bob", ImmutableSet.of(Action.READ)));
  }

  @Test
  public void testRevokeEntity() throws IOException, UnauthorizedException {
    NamespaceId ns1 = Ids.namespace("ns1");
    NamespaceId ns2 = Ids.namespace("ns2");

    // grant() and revoke(EntityId)
    client.grant(ns1, "admin", ImmutableSet.of(Action.READ));
    client.grant(ns1, "bob", ImmutableSet.of(Action.READ));
    client.grant(ns2, "admin", ImmutableSet.of(Action.READ));
    Assert.assertEquals(true, client.authorized(ns1, "admin", ImmutableSet.of(Action.READ)));
    Assert.assertEquals(true, client.authorized(ns1, "bob", ImmutableSet.of(Action.READ)));
    Assert.assertEquals(true, client.authorized(ns2, "admin", ImmutableSet.of(Action.READ)));
    client.revoke(ns1);
    Assert.assertEquals(false, client.authorized(ns1, "admin", ImmutableSet.of(Action.READ)));
    Assert.assertEquals(false, client.authorized(ns1, "bob", ImmutableSet.of(Action.READ)));
    Assert.assertEquals(true, client.authorized(ns2, "admin", ImmutableSet.of(Action.READ)));
  }
}
