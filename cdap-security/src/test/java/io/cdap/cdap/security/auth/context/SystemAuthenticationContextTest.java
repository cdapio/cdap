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
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SystemAuthenticationContext}.
 */
public class SystemAuthenticationContextTest {

  @After
  public void tearDown() {
    SecurityRequestContext.reset();
  }

  @Test
  public void testPrincipalPropagatesRequestCredentialsAndGeneratesSystemToken() throws Exception {
    Injector injector = Guice.createInjector(
        new IOModule(),
        new CoreSecurityRuntimeModule().getStandaloneModules(),
        new ConfigModule(CConfiguration.create()),
        new InMemoryDiscoveryModule());
    TokenManager tokenManager = injector.getInstance(TokenManager.class);
    AccessTokenCodec accessTokenCodec = injector.getInstance(AccessTokenCodec.class);
    tokenManager.startAndWait();

    try {
      final SystemAuthenticationContext context = new SystemAuthenticationContext(accessTokenCodec, tokenManager);

      Credential existingCred = new Credential("user-token", Credential.CredentialType.EXTERNAL_BEARER);
      SecurityRequestContext.setUserId("caller");
      SecurityRequestContext.setUserCredential(existingCred);
      Principal propagated = context.getPrincipal();
      Assert.assertEquals("caller", propagated.getName());
      Assert.assertEquals(existingCred, propagated.getFullCredential());

      SecurityRequestContext.reset();
      UserGroupInformation remoteUgi = UserGroupInformation.createRemoteUser("systemServiceUser");
      Principal generated = remoteUgi.doAs(new PrivilegedExceptionAction<Principal>() {
        @Override
        public Principal run() {
          return context.getPrincipal();
        }
      });

      Assert.assertEquals("systemServiceUser", generated.getName());
      Assert.assertNotNull(generated.getFullCredential());
      Assert.assertEquals(Credential.CredentialType.INTERNAL, generated.getFullCredential().getType());

      byte[] rawToken = Base64.getDecoder().decode(generated.getFullCredential().getValue());
      AccessToken decodedToken = accessTokenCodec.decode(rawToken);
      tokenManager.validateSecret(decodedToken);
      Assert.assertEquals("systemServiceUser", decodedToken.getIdentifier().getUsername());
      Assert.assertEquals(UserIdentity.IdentifierType.INTERNAL, decodedToken.getIdentifier().getIdentifierType());
    } finally {
      tokenManager.stopAndWait();
    }
  }
}
