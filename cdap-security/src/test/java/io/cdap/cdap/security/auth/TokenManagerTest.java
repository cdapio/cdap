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
import io.cdap.cdap.common.io.Codec;
import java.io.IOException;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link TokenManager}.
 */
public class TokenManagerTest {

  @Test
  public void testStartAndStopDelegatesToKeyManager() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    Injector injector = Guice.createInjector(new IOModule(), new ConfigModule(cConf));
    InMemoryKeyManager keyManager = new InMemoryKeyManager(cConf);
    TokenManager tokenManager = new TokenManager(keyManager, injector.getInstance(UserIdentityCodec.class));

    Assert.assertFalse(keyManager.isRunning());
    tokenManager.startAsync().awaitRunning();
    Assert.assertTrue(tokenManager.isRunning());
    Assert.assertTrue(keyManager.isRunning());

    long now = System.currentTimeMillis();
    UserIdentity identity = new UserIdentity(
        "bob", UserIdentity.IdentifierType.INTERNAL, Collections.emptyList(), now, now + 60000L);
    AccessToken token = tokenManager.signIdentifier(identity);
    tokenManager.validateSecret(token);

    tokenManager.stopAsync().awaitTerminated();
    Assert.assertEquals(Service.State.TERMINATED, tokenManager.state());
    Assert.assertEquals(Service.State.TERMINATED, keyManager.state());
  }

  @Test
  public void testSignIdentifierPropagatesCodecIoException() {
    InMemoryKeyManager keyManager = new InMemoryKeyManager(CConfiguration.create());
    final IOException expectedIoException = new IOException("encode failure");
    Codec<UserIdentity> failingCodec = new Codec<UserIdentity>() {
      @Override
      public byte[] encode(UserIdentity object) throws IOException {
        throw expectedIoException;
      }

      @Override
      public UserIdentity decode(byte[] data) {
        return null;
      }
    };

    TokenManager tokenManager = new TokenManager(keyManager, failingCodec);
    tokenManager.startAsync().awaitRunning();
    try {
      UserIdentity identity = new UserIdentity(
          "bob", UserIdentity.IdentifierType.EXTERNAL, Collections.emptyList(), 100L, 200L);
      try {
        tokenManager.signIdentifier(identity);
        Assert.fail("Expected RuntimeException wrapping IOException");
      } catch (RuntimeException e) {
        Assert.assertSame(expectedIoException, e.getCause());
      }
    } finally {
      tokenManager.stopAsync().awaitTerminated();
    }
  }
}
