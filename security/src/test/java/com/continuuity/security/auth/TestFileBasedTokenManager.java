package com.continuuity.security.auth;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.io.Codec;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.security.guice.FileBasedSecurityTestModule;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

/**
 * Tests for a key manager that saves keys to file.
 */
public class TestFileBasedTokenManager extends TestTokenManager {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Override
  protected ImmutablePair<TokenManager, Codec<AccessToken>> getTokenManagerAndCodec() {
    Injector injector = Guice.createInjector(new IOModule(), new ConfigModule(),
                                             new FileBasedSecurityTestModule(temporaryFolder),
                                             new DiscoveryRuntimeModule().getInMemoryModules());
    TokenManager tokenManager = injector.getInstance(TokenManager.class);
    tokenManager.startAndWait();
    Codec<AccessToken> tokenCodec = injector.getInstance(AccessTokenCodec.class);
    return new ImmutablePair<TokenManager, Codec<AccessToken>>(tokenManager, tokenCodec);
  }

  /**
   * Test that two token managers can share a key that is written to a file.
   * @throws Exception
   */
  @Test
  public void testFileBasedKey() throws Exception {
    ImmutablePair<TokenManager, Codec<AccessToken>> pair = getTokenManagerAndCodec();
    TokenManager tokenManager = pair.getFirst();
    Codec<AccessToken> tokenCodec = pair.getSecond();

    // Create a new token manager. This should not generate the key, but instead read the key from file.
    Injector injector = Guice.createInjector(new IOModule(), new ConfigModule(),
                                             new FileBasedSecurityTestModule(temporaryFolder),
                                             new DiscoveryRuntimeModule().getInMemoryModules());
    TokenManager tokenManager2 = injector.getInstance(TokenManager.class);
    tokenManager2.startAndWait();

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
