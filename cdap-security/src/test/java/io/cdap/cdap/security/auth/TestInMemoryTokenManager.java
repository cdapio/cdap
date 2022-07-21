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

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;

/**
 * Tests for InMemoryTokenManager that ensure that keys are maintained in memory and can be used to create
 * and validate AccessTokens.
 */
public class TestInMemoryTokenManager extends TestTokenManager {

  @Override
  protected ImmutablePair<TokenManager, Codec<AccessToken>> getTokenManagerAndCodec() {
    Injector injector = Guice.createInjector(new IOModule(), new CoreSecurityRuntimeModule().getStandaloneModules(),
                                             new ConfigModule(), new InMemoryDiscoveryModule());
    TokenManager tokenManager = injector.getInstance(TokenManager.class);
    tokenManager.startAndWait();
    Codec<AccessToken> tokenCodec = injector.getInstance(AccessTokenCodec.class);
    return new ImmutablePair<>(tokenManager, tokenCodec);
  }
}
