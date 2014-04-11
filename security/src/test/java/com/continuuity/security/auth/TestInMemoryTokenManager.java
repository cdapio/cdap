package com.continuuity.security.auth;

import com.continuuity.common.guice.IOModule;
import com.continuuity.security.guice.InMemorySecurityModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

/**
 *
 */
public class TestInMemoryTokenManager extends TestTokenManager {

  @BeforeClass
  public static void setup() throws Exception {
    Injector injector = Guice.createInjector(new IOModule(), new InMemorySecurityModule());
    tokenManager = injector.getInstance(TokenManager.class);
    tokenCodec = injector.getInstance(AccessTokenCodec.class);
  }
}
