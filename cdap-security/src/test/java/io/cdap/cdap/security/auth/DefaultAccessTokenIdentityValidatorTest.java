/*
 * Copyright © 2026 Cask Data, Inc.
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
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.spi.authentication.AccessTokenIdentityValidator;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authentication.ValidatedTokenIdentity;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link DefaultAccessTokenIdentityValidator}.
 */
public class DefaultAccessTokenIdentityValidatorTest {

  private static final long MINUTE_MILLIS = 60 * 1000L;

  private TokenManager tokenManager;
  private Codec<AccessToken> accessTokenCodec;
  private AccessTokenIdentityValidator validator;

  @Before
  public void setUp() {
    Injector injector = Guice.createInjector(
        new IOModule(),
        new ConfigModule(),
        new CoreSecurityRuntimeModule().getInMemoryModules());
    tokenManager = injector.getInstance(TokenManager.class);
    accessTokenCodec = injector.getInstance(Key.get(new TypeLiteral<Codec<AccessToken>>() {
    }));
    tokenManager.startAndWait();
    validator = injector.getInstance(AccessTokenIdentityValidator.class);
  }

  @After
  public void tearDown() {
    if (tokenManager.isRunning()) {
      tokenManager.stopAndWait();
    }
  }

  @Test
  public void testValidInternalToken() throws IOException {
    long now = System.currentTimeMillis();
    UserIdentity identity = new UserIdentity(
        "system", UserIdentity.IdentifierType.INTERNAL,
        Collections.emptyList(), now, now + 5 * MINUTE_MILLIS);
    String encoded = Base64.getEncoder()
        .encodeToString(accessTokenCodec.encode(tokenManager.signIdentifier(identity)));

    ValidatedTokenIdentity validated = validator.validate(encoded);
    Assert.assertEquals("system", validated.getUsername());
    Assert.assertTrue(validated.isInternal());
  }

  @Test
  public void testValidExternalToken() throws IOException {
    long now = System.currentTimeMillis();
    UserIdentity identity = new UserIdentity(
        "alice", UserIdentity.IdentifierType.EXTERNAL,
        Collections.emptyList(), now, now + 5 * MINUTE_MILLIS);
    String encoded = Base64.getEncoder()
        .encodeToString(accessTokenCodec.encode(tokenManager.signIdentifier(identity)));

    ValidatedTokenIdentity validated = validator.validate(encoded);
    Assert.assertEquals("alice", validated.getUsername());
    Assert.assertFalse(validated.isInternal());
  }

  @Test(expected = UnauthenticatedException.class)
  public void testExpiredTokenThrows() throws IOException {
    long now = System.currentTimeMillis();
    UserIdentity identity = new UserIdentity(
        "alice", UserIdentity.IdentifierType.EXTERNAL,
        Collections.emptyList(), now - 10 * MINUTE_MILLIS, now - MINUTE_MILLIS);
    String encoded = Base64.getEncoder()
        .encodeToString(accessTokenCodec.encode(tokenManager.signIdentifier(identity)));
    validator.validate(encoded);
  }

  @Test(expected = UnauthenticatedException.class)
  public void testForgedDigestThrows() throws IOException {
    long now = System.currentTimeMillis();
    UserIdentity identity = new UserIdentity(
        "admin", UserIdentity.IdentifierType.INTERNAL,
        Collections.emptyList(), now, now + 5 * MINUTE_MILLIS);
    AccessToken forged = new AccessToken(
        identity, 0, "forged-digest".getBytes(StandardCharsets.UTF_8));
    String encoded = Base64.getEncoder().encodeToString(accessTokenCodec.encode(forged));
    validator.validate(encoded);
  }

  @Test(expected = UnauthenticatedException.class)
  public void testMalformedTokenThrows() {
    validator.validate("not-valid-base64!!!");
  }

  @Test(expected = UnauthenticatedException.class)
  public void testDummyTokenThrows() {
    String dummy = Base64.getEncoder()
        .encodeToString("dummy".getBytes(StandardCharsets.UTF_8));
    validator.validate(dummy);
  }
}
