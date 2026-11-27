/*
 * Copyright © 2018 Cask Data, Inc.
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
package io.cdap.cdap.security.authorization;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.InMemoryOwnerStore;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.net.InetAddress;
import java.util.Collections;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for {@link AuthorizationUtil}
 */
public class AuthorizationUtilTest {
  private static CConfiguration cConf;
  private static InMemoryNamespaceAdmin namespaceClient;
  private static AuthenticationContext authenticationContext;
  private static final NamespaceId namespaceId = new NamespaceId("AuthorizationUtilTest");
  private static final ApplicationId applicationId = namespaceId.app("someapp");
  private static String username;

  @BeforeClass
  public static void init() throws Exception {
    cConf = CConfiguration.create();
    // Note: it is important to initialize the UGI before we call AuthorizationUtil.getAppAuthorizingUser(..)
    // which uses KerberosName since KerberosName expect the rules for matching to be set. See
    // http://lucene.472066.n3.nabble.com/KerberosName-rules-are-null-during-KerberosName-getShortName-
    // in-KerberosAuthenticationHandler-td4074309.html for more context
    username = UserGroupInformation.getCurrentUser().getShortUserName();
    namespaceClient = new InMemoryNamespaceAdmin();
    authenticationContext = new AuthenticationTestContext();
  }

  @Test
  public void testGetAppAuthorizingUse() throws Exception {
    OwnerAdmin ownerAdmin = getOwnerAdmin();
    // test with complete principal (alice/somehost.net@somerealm.net)
    String principal = username + "/" + InetAddress.getLocalHost().getHostName() + "@REALM.net";
    NamespaceMeta nsMeta = new NamespaceMeta.Builder().setName(namespaceId).setPrincipal(principal)
      .setKeytabUri("doesnotmatter").build();
    namespaceClient.create(nsMeta);
    Assert.assertEquals(username, AuthorizationUtil.getAppAuthorizingUser(ownerAdmin, authenticationContext,
                                                                          applicationId, null));

    // test with principal which is just username (alice)
    namespaceClient.delete(namespaceId);
    principal = username;
    nsMeta = new NamespaceMeta.Builder().setName(namespaceId).setPrincipal(principal)
      .setKeytabUri("doesnotmatter").build();
    namespaceClient.create(nsMeta);
    Assert.assertEquals(username, AuthorizationUtil.getAppAuthorizingUser(ownerAdmin, authenticationContext,
                                                                          applicationId, null));

    // test with principal and realm (alice@somerealm.net)
    namespaceClient.delete(namespaceId);
    principal = username + "@REALM.net";
    nsMeta = new NamespaceMeta.Builder().setName(namespaceId).setPrincipal(principal)
      .setKeytabUri("doesnotmatter").build();
    namespaceClient.create(nsMeta);
    Assert.assertEquals(username, AuthorizationUtil.getAppAuthorizingUser(ownerAdmin, authenticationContext,
                                                                          applicationId, null));
    // clean up
    namespaceClient.delete(namespaceId);
  }

  private OwnerAdmin getOwnerAdmin() {
    return new DefaultOwnerAdmin(cConf, new InMemoryOwnerStore(), namespaceClient);
  }

  @Test
  public void testEnforceAnyNamespaceAccess() throws Exception {
    Principal instanceReader = new Principal("any-ns-instance-reader", Principal.PrincipalType.USER);
    final Principal namespaceUser = new Principal("any-ns-namespace-user", Principal.PrincipalType.USER);
    final Principal instanceAccessOnly = new Principal("any-ns-instance-access-only", Principal.PrincipalType.USER);
    InMemoryAccessController accessController = new InMemoryAccessController();
    accessController.grant(Authorizable.fromEntityId(InstanceId.SELF, EntityType.NAMESPACE), instanceReader,
                           Collections.singleton(StandardPermission.LIST));
    DefaultContextAccessEnforcer enforcer = new DefaultContextAccessEnforcer(authenticationContext,
                                                                             accessController);

    // An instance-level reader passes without listing namespaces.
    NamespaceQueryAdmin noNamespaces = mock(NamespaceQueryAdmin.class);
    when(noNamespaces.list()).thenReturn(Collections.emptyList());
    AuthenticationTestContext.actAsPrincipal(instanceReader);
    AuthorizationUtil.enforceAnyNamespaceAccess(noNamespaces, enforcer);
    AuthorizationUtil.enforceInstanceLevelRead(enforcer);
    verify(noNamespaces, never()).list();

    // A caller that can see a namespace passes the "any namespace" check, but not the instance check.
    NamespaceQueryAdmin oneNamespace = mock(NamespaceQueryAdmin.class);
    when(oneNamespace.list()).thenReturn(Collections.singletonList(NamespaceMeta.DEFAULT));
    AuthenticationTestContext.actAsPrincipal(namespaceUser);
    AuthorizationUtil.enforceAnyNamespaceAccess(oneNamespace, enforcer);
    try {
      AuthorizationUtil.enforceInstanceLevelRead(enforcer);
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }

    // A caller that can only reach the instance fails both.
    AuthenticationTestContext.actAsPrincipal(instanceAccessOnly);
    try {
      AuthorizationUtil.enforceAnyNamespaceAccess(noNamespaces, enforcer);
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
  }
}
