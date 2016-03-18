/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.common.FeatureDisabledException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.authorization.AuthorizerInstantiatorService;
import co.cask.cdap.security.authorization.InvalidAuthorizerException;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;

/**
 * Tests for {@link AuthorizationHandler}.
 */
public class AuthorizationHandlerTest {

  private NettyHttpService service;
  private AuthorizationClient client;

  @Before
  public void setUp() throws UnknownHostException {
    CConfiguration conf = CConfiguration.create();
    conf.setBoolean(Constants.Security.Authorization.ENABLED, true);

    final InMemoryAuthorizer auth = new InMemoryAuthorizer();
    service = new CommonNettyHttpServiceBuilder(conf)
      .addHttpHandlers(ImmutableList.of(new AuthorizationHandler(new AuthorizerInstantiatorService(conf) {
        @Override
        public Authorizer get() throws IOException, InvalidAuthorizerException {
          return auth;
        }
      }, conf)))
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
  public void testDisabled() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.setBoolean(Constants.Security.Authorization.ENABLED, false);

    NettyHttpService service = new CommonNettyHttpServiceBuilder(conf)
      .addHttpHandlers(ImmutableList.of(new AuthorizationHandler(new AuthorizerInstantiatorService(conf) {
        @Override
        public Authorizer get() throws IOException, InvalidAuthorizerException {
          return new InMemoryAuthorizer();
        }
      }, conf)))
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
    Principal admin = new Principal("admin", Principal.PrincipalType.USER);
    try {
      client.authorized(ns1, admin, Action.READ);
      Assert.fail();
    } catch (FeatureDisabledException expected) {
      Assert.assertEquals("Authorization", expected.getFeature());
    }

    try {
      client.grant(ns1, admin, ImmutableSet.of(Action.READ));
      Assert.fail();
    } catch (FeatureDisabledException expected) {
      Assert.assertEquals(Constants.Security.Authorization.ENABLED, expected.getEnableConfigKey());
      Assert.assertEquals("true", expected.getEnableConfigValue());
    }

    try {
      client.revoke(ns1, admin, ImmutableSet.of(Action.READ));
      Assert.fail();
    } catch (FeatureDisabledException expected) {
      Assert.assertEquals("cdap-site.xml", expected.getConfigFile());
    }
  }

  @Test
  public void testRevokeEntityUserActions() throws Exception {
    NamespaceId ns1 = Ids.namespace("ns1");
    Principal admin = new Principal("admin", Principal.PrincipalType.ROLE);

    // grant() and revoke(EntityId, String, Set<Action>)
    verifyAuthFailure(ns1, admin, Action.READ);

    client.grant(ns1, admin, ImmutableSet.of(Action.READ));
    client.authorized(ns1, admin, Action.READ);

    client.revoke(ns1, admin, ImmutableSet.of(Action.READ));
    verifyAuthFailure(ns1, admin, Action.READ);
  }

  @Test
  public void testRevokeEntityUser() throws Exception {
    NamespaceId ns1 = Ids.namespace("ns1");
    Principal admin = new Principal("admin", Principal.PrincipalType.GROUP);
    Principal bob = new Principal("bob", Principal.PrincipalType.USER);

    // grant() and revoke(EntityId, String)
    client.grant(ns1, admin, ImmutableSet.of(Action.READ));
    client.grant(ns1, bob, ImmutableSet.of(Action.READ));
    client.authorized(ns1, admin, Action.READ);
    client.authorized(ns1, bob, Action.READ);

    client.revoke(ns1, admin);
    verifyAuthFailure(ns1, admin, Action.READ);
    client.authorized(ns1, bob, Action.READ);
  }

  @Test
  public void testRevokeEntity() throws Exception {
    NamespaceId ns1 = Ids.namespace("ns1");
    NamespaceId ns2 = Ids.namespace("ns2");
    Principal admin = new Principal("admin", Principal.PrincipalType.GROUP);
    Principal bob = new Principal("bob", Principal.PrincipalType.USER);

    // grant() and revoke(EntityId)
    client.grant(ns1, admin, ImmutableSet.of(Action.READ));
    client.grant(ns1, bob, ImmutableSet.of(Action.READ));
    client.grant(ns2, admin, ImmutableSet.of(Action.READ));
    client.authorized(ns1, admin, Action.READ);
    client.authorized(ns1, bob, Action.READ);
    client.authorized(ns2, admin, Action.READ);

    client.revoke(ns1);
    verifyAuthFailure(ns1, admin, Action.READ);
    verifyAuthFailure(ns1, bob, Action.READ);
    client.authorized(ns2, admin, Action.READ);
  }

  private void verifyAuthFailure(EntityId entity, Principal principal, Action action) throws Exception {
    try {
      client.authorized(entity, principal, action);
      Assert.fail(String.format("Expected authorization failure, but it succeeded for entity %s, principal %s," +
                                  " action %s", entity, principal, action));
    } catch (UnauthorizedException expected) {
      // expected
    }
  }
}
