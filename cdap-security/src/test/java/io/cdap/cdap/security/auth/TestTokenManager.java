/*
 * Copyright © 2014-2021 Cask Data, Inc.
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
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.utils.ImmutablePair;
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
public abstract class TestTokenManager {
  protected static final Logger LOG = LoggerFactory.getLogger(TestTokenManager.class);
  protected static final long TOKEN_DURATION = 3600 * 1000;

  protected abstract ImmutablePair<TokenManager, Codec<AccessToken>> getTokenManagerAndCodec() throws Exception;

  @Test
  public void testTokenValidation() throws Exception {
    ImmutablePair<TokenManager, Codec<AccessToken>> pair = getTokenManagerAndCodec();
    TokenManager tokenManager = pair.getFirst();
    tokenManager.startAndWait();
    Codec<AccessToken> tokenCodec = pair.getSecond();

    long now = System.currentTimeMillis();
    String user = "testuser";
    List<String> groups = Lists.newArrayList("users", "admins");
    UserIdentity ident1 = new UserIdentity(user, UserIdentity.IdentifierType.EXTERNAL, groups,
                                           now, now + TOKEN_DURATION);
    AccessToken token1 = tokenManager.signIdentifier(ident1);
    LOG.info("Signed token is: " + Bytes.toStringBinary(tokenCodec.encode(token1)));
    // should be valid since we just signed it
    tokenManager.validateSecret(token1);

    // test token expiration
    UserIdentity expiredIdent = new UserIdentity(user, UserIdentity.IdentifierType.EXTERNAL, groups, now - 1000,
                                                 now - 1);
    AccessToken expiredToken = tokenManager.signIdentifier(expiredIdent);
    try {
      tokenManager.validateSecret(expiredToken);
      fail("Token should have been expired but passed validation: " +
             Bytes.toStringBinary(tokenCodec.encode(expiredToken)));
    } catch (InvalidTokenException expected) {
      // expected
    }

    // test token with invalid signature
    Random random = new Random();
    byte[] invalidDigest = token1.getDigestBytes();
    random.nextBytes(invalidDigest);
    AccessToken invalidToken = new AccessToken(token1.getIdentifier(), token1.getKeyId(), invalidDigest);
    try {
      tokenManager.validateSecret(invalidToken);
      fail("Token should have been rejected for invalid digest but passed: " +
             Bytes.toStringBinary(tokenCodec.encode(invalidToken)));
    } catch (InvalidTokenException expected) {
      // expected
    }

    // test token with bad key ID
    AccessToken invalidKeyToken = new AccessToken(token1.getIdentifier(), token1.getKeyId() + 1,
                                                  token1.getDigestBytes());
    try {
      tokenManager.validateSecret(invalidKeyToken);
      fail("Token should have been rejected for invalid key ID but passed: " +
             Bytes.toStringBinary(tokenCodec.encode(invalidToken)));
    } catch (InvalidTokenException expected) {
      // expected
    }

    tokenManager.stopAndWait();
  }

  @Test
  public void testTokenSerialization() throws Exception {
    ImmutablePair<TokenManager, Codec<AccessToken>> pair = getTokenManagerAndCodec();
    TokenManager tokenManager = pair.getFirst();
    tokenManager.startAndWait();
    Codec<AccessToken> tokenCodec = pair.getSecond();

    long now = System.currentTimeMillis();
    String user = "testuser";
    List<String> groups = Lists.newArrayList("users", "admins");
    UserIdentity ident1 = new UserIdentity(user, UserIdentity.IdentifierType.EXTERNAL, groups,
                                           now, now + TOKEN_DURATION);
    AccessToken token1 = tokenManager.signIdentifier(ident1);
    byte[] tokenBytes = tokenCodec.encode(token1);

    AccessToken token2 = tokenCodec.decode(tokenBytes);

    assertEquals(token1, token2);
    LOG.info("Deserialized token is: " + Bytes.toStringBinary(tokenCodec.encode(token2)));
    // should be valid since we just signed it
    tokenManager.validateSecret(token2);

    tokenManager.stopAndWait();
  }
}
