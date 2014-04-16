package com.continuuity.security.auth;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.security.guice.FileBasedSecurityTestModule;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

/**
 * Tests for a key manager that saves keys to file.
 */
public class TestFIleBasedKeyManager extends TestTokenManager {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    Injector injector = Guice.createInjector(new IOModule(), new ConfigModule(),
                                             new FileBasedSecurityTestModule(temporaryFolder));
    tokenManager = injector.getInstance(TokenManager.class);
    tokenCodec = injector.getInstance(AccessTokenCodec.class);
  }

  /**
   * Test that two token managers can share a key that is written to a file.
   * @throws Exception
   */
  @Test
  public void testFileBasedKey() throws Exception {
    // Create a new token manager. This should not generate the key, but instead read the key from file.
    Injector injector = Guice.createInjector(new IOModule(), new ConfigModule(),
                                             new FileBasedSecurityTestModule(temporaryFolder));
    TokenManager tokenManager2 = injector.getInstance(TokenManager.class);

    Assert.assertNotSame("ERROR: Both token managers refer to the same object.", tokenManager, tokenManager2);

    String user = "testuser";
    long now = System.currentTimeMillis();
    List<String> groups = Lists.newArrayList("users", "admins");
    AccessTokenIdentifier identifier = new AccessTokenIdentifier(user, groups, now, now + TOKEN_DURATION);

    AccessToken token = tokenManager.signIdentifier(identifier);
    LOG.info("Signed token is: {}.", Bytes.toStringBinary(tokenCodec.encode(token)));

    // Since both tokenManagers have the same key, they must both be able to validate the secret.
    tokenManager.validateSecret(token);
    tokenManager2.validateSecret(token);
  }
}
