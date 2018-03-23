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
package co.cask.cdap.security.authorization;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.namespace.InMemoryNamespaceClient;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationTestContext;
import co.cask.cdap.security.impersonation.DefaultOwnerAdmin;
import co.cask.cdap.security.impersonation.InMemoryOwnerStore;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;

/**
 * Test for {@link AuthorizationUtil}
 */
public class AuthorizationUtilTest {
  private static CConfiguration cConf;
  private static InMemoryNamespaceClient namespaceClient;
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
    namespaceClient = new InMemoryNamespaceClient();
    authenticationContext = new AuthenticationTestContext();
  }

  @Test
  public void testGetAppAuthorizingUse() throws Exception {
    OwnerAdmin ownerAdmin = getOwnerAdmin();
    // test with complete principal (alice/somehost.net@somerealm.net)
    String principal = username + "/" + InetAddress.getLocalHost().getHostName() + "@REALM.net";
    NamespaceMeta nsMeta = new NamespaceMeta.Builder().setName(namespaceId).setPrincipal(principal)
      .setKeytabURI("doesnotmatter").build();
    namespaceClient.create(nsMeta);
    Assert.assertEquals(username, AuthorizationUtil.getAppAuthorizingUser(ownerAdmin, authenticationContext,
                                                                          applicationId, null));

    // test with principal which is just username (alice)
    namespaceClient.delete(namespaceId);
    principal = username;
    nsMeta = new NamespaceMeta.Builder().setName(namespaceId).setPrincipal(principal)
      .setKeytabURI("doesnotmatter").build();
    namespaceClient.create(nsMeta);
    Assert.assertEquals(username, AuthorizationUtil.getAppAuthorizingUser(ownerAdmin, authenticationContext,
                                                                          applicationId, null));

    // test with principal and realm (alice@somerealm.net)
    namespaceClient.delete(namespaceId);
    principal = username + "@REALM.net";
    nsMeta = new NamespaceMeta.Builder().setName(namespaceId).setPrincipal(principal)
      .setKeytabURI("doesnotmatter").build();
    namespaceClient.create(nsMeta);
    Assert.assertEquals(username, AuthorizationUtil.getAppAuthorizingUser(ownerAdmin, authenticationContext,
                                                                          applicationId, null));
    // clean up
    namespaceClient.delete(namespaceId);
  }

  private OwnerAdmin getOwnerAdmin() {
    return new DefaultOwnerAdmin(cConf, new InMemoryOwnerStore(), namespaceClient);
  }
}
