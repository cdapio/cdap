package com.continuuity.security.auth;

import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.security.guice.InMemorySecurityModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

/**
 * Tests for InMemoryTokenManager that ensure that keys are maintained in memory and can be used to create
 * and validate AccessTokens.
 */
public class TestInMemoryTokenManager extends TestTokenManager {

  @Override
  @BeforeClass
  public void setup() throws Exception {
    Injector injector = Guice.createInjector(new IOModule(), new InMemorySecurityModule(), new ConfigModule());
    tokenManager = injector.getInstance(TokenManager.class);
    tokenCodec = injector.getInstance(AccessTokenCodec.class);
  }
}
