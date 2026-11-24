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

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.common.internal.remote.NoOpInternalAuthenticator;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AuthenticationContextModules}.
 */
public class AuthenticationContextModulesTest {

  @Test
  public void testProgramContainerModuleWithKerberosPrincipalUsesCurrentUgiUser() throws Exception {
    final CConfiguration cConf = CConfiguration.create();
    final AuthenticationContextModules modules = new AuthenticationContextModules();
    UserGroupInformation remoteUgi = UserGroupInformation.createRemoteUser("containerUser");

    Principal principal = remoteUgi.doAs(new PrivilegedExceptionAction<Principal>() {
      @Override
      public Principal run() {
        Injector injector = Guice.createInjector(
            new ConfigModule(cConf),
            modules.getProgramContainerModule(cConf, "containerUser/host@REALM"));
        AuthenticationContext authContext = injector.getInstance(AuthenticationContext.class);
        InternalAuthenticator internalAuth = injector.getInstance(InternalAuthenticator.class);
        Assert.assertTrue(internalAuth instanceof NoOpInternalAuthenticator);
        return authContext.getPrincipal();
      }
    });

    Assert.assertEquals("containerUser", principal.getName());
    Assert.assertEquals(Principal.PrincipalType.USER, principal.getType());
    Assert.assertEquals("containerUser/host@REALM", principal.getKerberosPrincipal());
  }

  @Test
  public void testProgramContainerModuleWithAndWithoutRuntimeToken() throws Exception {
    final CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Security.Authentication.RUNTIME_TOKEN, "secret-runtime-token");
    final AuthenticationContextModules modules = new AuthenticationContextModules();
    UserGroupInformation remoteUgi = UserGroupInformation.createRemoteUser("tokenUser");

    Principal principal = remoteUgi.doAs(new PrivilegedExceptionAction<Principal>() {
      @Override
      public Principal run() {
        Injector injector = Guice.createInjector(
            new ConfigModule(cConf),
            modules.getProgramContainerModule(cConf));
        return injector.getInstance(AuthenticationContext.class).getPrincipal();
      }
    });

    Assert.assertEquals("tokenUser", principal.getName());
    Assert.assertNotNull(principal.getFullCredential());
    Assert.assertEquals("secret-runtime-token", principal.getFullCredential().getValue());
    Assert.assertEquals(Credential.CredentialType.INTERNAL, principal.getFullCredential().getType());

    CConfiguration noTokenConf = CConfiguration.create();
    Injector noTokenInjector = Guice.createInjector(
        new ConfigModule(noTokenConf),
        modules.getProgramContainerModule(noTokenConf));
    Assert.assertTrue(
        noTokenInjector.getInstance(AuthenticationContext.class) instanceof MasterAuthenticationContext);
  }

  @Test
  public void testMasterAndNoOpModules() {
    CConfiguration cConf = CConfiguration.create();
    AuthenticationContextModules modules = new AuthenticationContextModules();

    Injector masterInjector = Guice.createInjector(new ConfigModule(cConf), modules.getMasterModule());
    Assert.assertTrue(masterInjector.getInstance(AuthenticationContext.class) instanceof MasterAuthenticationContext);

    Injector noOpInjector = Guice.createInjector(new ConfigModule(cConf), modules.getNoOpModule());
    Assert.assertTrue(noOpInjector.getInstance(AuthenticationContext.class) instanceof AuthenticationTestContext);
  }
}
