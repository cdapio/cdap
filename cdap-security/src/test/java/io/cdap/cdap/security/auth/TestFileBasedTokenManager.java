/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.security.guice.FileBasedSecurityModule;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;

/**
 * Tests for a key manager that saves keys to file.
 */
public class TestFileBasedTokenManager extends TestTokenManager {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Override
  protected ImmutablePair<TokenManager, Codec<AccessToken>> getTokenManagerAndCodec() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    Injector injector = Guice.createInjector(new IOModule(),
                                             new ConfigModule(cConf),
                                             new FileBasedSecurityModule(),
                                             new InMemoryDiscoveryModule());
    TokenManager tokenManager = injector.getInstance(TokenManager.class);
    tokenManager.startAndWait();
    Codec<AccessToken> tokenCodec = injector.getInstance(AccessTokenCodec.class);
    return new ImmutablePair<>(tokenManager, tokenCodec);
  }

  /**
   * Test that two token managers can share a key that is written to a file.
   * @throws Exception
   */
  @Test
  public void testFileBasedKey() throws Exception {
    // Create two token managers that points to the same path
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    TokenManager tokenManager = Guice.createInjector(
      new IOModule(),
      new ConfigModule(cConf),
      new FileBasedSecurityModule(),
      new InMemoryDiscoveryModule()).getInstance(TokenManager.class);
    tokenManager.startAndWait();

    TokenManager tokenManager2 = Guice.createInjector(
      new IOModule(),
      new ConfigModule(cConf),
      new FileBasedSecurityModule(),
      new InMemoryDiscoveryModule()).getInstance(TokenManager.class);
    tokenManager2.startAndWait();

    Assert.assertNotSame("ERROR: Both token managers refer to the same object.", tokenManager, tokenManager2);

    String user = "testuser";
    long now = System.currentTimeMillis();
    List<String> groups = Lists.newArrayList("users", "admins");
    UserIdentity identifier = new UserIdentity(user, groups, now, now + TOKEN_DURATION);

    AccessToken token = tokenManager.signIdentifier(identifier);

    // Since both tokenManagers have the same key, they must both be able to validate the secret.
    tokenManager.validateSecret(token);
    tokenManager2.validateSecret(token);
  }
}
