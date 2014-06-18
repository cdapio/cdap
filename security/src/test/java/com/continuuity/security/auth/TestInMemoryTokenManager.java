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
