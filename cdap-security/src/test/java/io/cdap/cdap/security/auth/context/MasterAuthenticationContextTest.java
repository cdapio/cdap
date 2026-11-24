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

package io.cdap.cdap.security.auth.context;

import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link MasterAuthenticationContext}.
 */
public class MasterAuthenticationContextTest {

  @After
  public void tearDown() {
    SecurityRequestContext.reset();
  }

  @Test
  public void testPrincipalFromSecurityRequestContext() {
    Credential credential = new Credential("bearer-token", Credential.CredentialType.EXTERNAL_BEARER);
    SecurityRequestContext.setUserId("requestUser");
    SecurityRequestContext.setUserCredential(credential);

    MasterAuthenticationContext context = new MasterAuthenticationContext();
    Principal principal = context.getPrincipal();

    Assert.assertEquals("requestUser", principal.getName());
    Assert.assertEquals(Principal.PrincipalType.USER, principal.getType());
    Assert.assertEquals(credential, principal.getFullCredential());
  }

  @Test
  public void testPrincipalFallsBackToCurrentUgiWhenRequestUserIdNull() throws Exception {
    SecurityRequestContext.reset();
    final MasterAuthenticationContext context = new MasterAuthenticationContext();
    UserGroupInformation remoteUgi = UserGroupInformation.createRemoteUser("masterDaemonUser");

    Principal principal = remoteUgi.doAs(new PrivilegedExceptionAction<Principal>() {
      @Override
      public Principal run() {
        return context.getPrincipal();
      }
    });

    Assert.assertEquals("masterDaemonUser", principal.getName());
    Assert.assertEquals(Principal.PrincipalType.USER, principal.getType());
    Assert.assertNull(principal.getFullCredential());
  }
}
