/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.security.auth;

import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.io.Codec;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.security.guice.InMemorySecurityModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Tests for InMemoryTokenManager that ensure that keys are maintained in memory and can be used to create
 * and validate AccessTokens.
 */
public class TestInMemoryTokenManager extends TestTokenManager {

  @Override
  protected ImmutablePair<TokenManager, Codec<AccessToken>> getTokenManagerAndCodec() {
    Injector injector = Guice.createInjector(new IOModule(), new InMemorySecurityModule(), new ConfigModule(),
                                             new DiscoveryRuntimeModule().getInMemoryModules());
    TokenManager tokenManager = injector.getInstance(TokenManager.class);
    tokenManager.startAndWait();
    Codec<AccessToken> tokenCodec = injector.getInstance(AccessTokenCodec.class);
    return new ImmutablePair<TokenManager, Codec<AccessToken>>(tokenManager, tokenCodec);
  }
}
