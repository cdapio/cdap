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

package io.cdap.cdap.security.auth;

import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AccessTokenValidator}.
 */
public class AccessTokenValidatorTest {

  @Test
  public void testStartStopAndValidationStates() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    Injector injector = Guice.createInjector(
        new IOModule(),
        new CoreSecurityRuntimeModule().getStandaloneModules(),
        new ConfigModule(cConf),
        new InMemoryDiscoveryModule());
    TokenManager tokenManager = injector.getInstance(TokenManager.class);
    AccessTokenCodec accessTokenCodec = injector.getInstance(AccessTokenCodec.class);
    AccessTokenValidator validator = new AccessTokenValidator(tokenManager, accessTokenCodec);

    Assert.assertFalse(tokenManager.isRunning());
    validator.startAsync().awaitRunning();
    Assert.assertTrue(validator.isRunning());
    Assert.assertTrue(tokenManager.isRunning());

    try {
      Assert.assertEquals(TokenState.MISSING, validator.validate(null));

      long now = System.currentTimeMillis();
      UserIdentity validIdentity = new UserIdentity(
          "alice", UserIdentity.IdentifierType.EXTERNAL, Collections.singletonList("users"), now, now + 60000L);
      AccessToken validToken = tokenManager.signIdentifier(validIdentity);
      String encodedValid = Base64.getEncoder().encodeToString(accessTokenCodec.encode(validToken));
      Assert.assertEquals(TokenState.VALID, validator.validate(encodedValid));

      UserIdentity expiredIdentity = new UserIdentity(
          "alice", UserIdentity.IdentifierType.EXTERNAL, Collections.singletonList("users"), now - 2000L, now - 1000L);
      AccessToken expiredToken = tokenManager.signIdentifier(expiredIdentity);
      String encodedExpired = Base64.getEncoder().encodeToString(accessTokenCodec.encode(expiredToken));
      Assert.assertEquals(TokenState.EXPIRED, validator.validate(encodedExpired));

      String corruptToken = Base64.getEncoder().encodeToString("not-a-valid-token".getBytes(StandardCharsets.UTF_8));
      Assert.assertEquals(TokenState.INVALID, validator.validate(corruptToken));
    } finally {
      validator.stopAsync().awaitTerminated();
    }

    Assert.assertEquals(Service.State.TERMINATED, validator.state());
    Assert.assertEquals(Service.State.TERMINATED, tokenManager.state());
  }
}
