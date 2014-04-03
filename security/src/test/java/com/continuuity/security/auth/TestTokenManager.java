package com.continuuity.security.auth;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.guice.IOModule;
import com.continuuity.security.guice.SecurityModule;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests related to {@link TokenManager} implementations.
 */
public class TestTokenManager {
  private static final Logger LOG = LoggerFactory.getLogger(TestTokenManager.class);
  private static final long tokenDuration = 3600 * 1000;
  private static TokenManager tokenManager;
  private static AccessTokenCodec tokenCodec;

  @BeforeClass
  public static void setup() throws Exception {
    Injector injector = Guice.createInjector(new IOModule(), new SecurityModule());
    tokenManager = injector.getInstance(TokenManager.class);
    tokenCodec = injector.getInstance(AccessTokenCodec.class);
  }

  @Test
  public void testTokenValidation() throws Exception {
    long now = System.currentTimeMillis();
    String user = "testuser";
    List<String> groups = Lists.newArrayList("users", "admins");
    AccessTokenIdentifier ident1 = new AccessTokenIdentifier(user, groups,
                                                             now, now + tokenDuration);
    AccessToken token1 = tokenManager.signIdentifier(ident1);
    LOG.info("Signed token is: " + Bytes.toStringBinary(tokenCodec.encode(token1)));
    // should be valid since we just signed it
    tokenManager.validateSecret(token1);

    // test token expiration
    AccessTokenIdentifier expiredIdent = new AccessTokenIdentifier(user, groups, now - 1000, now - 1);
    AccessToken expiredToken = tokenManager.signIdentifier(expiredIdent);
    try {
      tokenManager.validateSecret(expiredToken);
      fail("Token should have been expired but passed validation: " +
             Bytes.toStringBinary(tokenCodec.encode(expiredToken)));
    } catch (InvalidTokenException expected) {}

    // test token with invalid signature
    Random random = new Random();
    byte[] invalidDigest = token1.getDigestBytes();
    random.nextBytes(invalidDigest);
    AccessToken invalidToken = new AccessToken(token1.getIdentifier(), token1.getKeyId(), invalidDigest);
    try {
      tokenManager.validateSecret(invalidToken);
      fail("Token should have been rejected for invalid digest but passed: " +
             Bytes.toStringBinary(tokenCodec.encode(invalidToken)));
    } catch (InvalidTokenException expected) {}

    // test token with bad key ID
    AccessToken invalidKeyToken = new AccessToken(token1.getIdentifier(), token1.getKeyId() + 1,
                                                  token1.getDigestBytes());
    try {
      tokenManager.validateSecret(invalidKeyToken);
      fail("Token should have been rejected for invalid key ID but passed: " +
             Bytes.toStringBinary(tokenCodec.encode(invalidToken)));
    } catch (InvalidTokenException expected) {}
  }

  @Test
  public void testTokenSerialization() throws Exception {
    long now = System.currentTimeMillis();
    String user = "testuser";
    List<String> groups = Lists.newArrayList("users", "admins");
    AccessTokenIdentifier ident1 = new AccessTokenIdentifier(user, groups,
                                                             now, now + tokenDuration);
    AccessToken token1 = tokenManager.signIdentifier(ident1);
    byte[] tokenBytes = tokenCodec.encode(token1);

    AccessToken token2 = tokenCodec.decode(tokenBytes);

    assertEquals(token1, token2);
    LOG.info("Deserialzied token is: " + Bytes.toStringBinary(tokenCodec.encode(token2)));
    // should be valid since we just signed it
    tokenManager.validateSecret(token2);
  }
}
