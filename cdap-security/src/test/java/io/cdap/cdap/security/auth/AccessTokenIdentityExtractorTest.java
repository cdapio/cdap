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

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.service.Services;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link AccessTokenIdentityExtractor}.
 */
public class AccessTokenIdentityExtractorTest {

  private static final long TOKEN_TTL_MILLIS = TimeUnit.MINUTES.toMillis(5);

  private TokenManager tokenManager;
  private Codec<AccessToken> accessTokenCodec;
  private AccessTokenIdentityExtractor extractor;

  @Before
  public void setUp() {
    Injector injector = Guice.createInjector(
        new IOModule(),
        new ConfigModule(),
        new CoreSecurityRuntimeModule().getInMemoryModules());
    tokenManager = injector.getInstance(TokenManager.class);
    Services.startAndWait(tokenManager);
    accessTokenCodec = injector.getInstance(Key.get(new TypeLiteral<Codec<AccessToken>>() { }));
    TokenValidator tokenValidator = injector.getInstance(TokenValidator.class);
    UserIdentityCodec userIdentityCodec = injector.getInstance(UserIdentityCodec.class);
    extractor = new AccessTokenIdentityExtractor(tokenValidator, accessTokenCodec, userIdentityCodec);
  }

  @After
  public void tearDown() {
    if (tokenManager != null) {
      Services.stopAndWait(tokenManager);
    }
  }

  @Test
  public void testExtractRuntimeTokenRejected() throws Exception {
    String encodedToken = createSignedToken("run-user", UserIdentity.IdentifierType.RUNTIME);
    for (String prefix : ImmutableList.of("Bearer ", "CDAP-Internal ")) {
      UserIdentityExtractionResponse response =
          extractor.extract(createRequest(prefix + encodedToken));

      Assert.assertFalse("Expected failure for prefix: " + prefix, response.success());
      Assert.assertEquals("Unexpected state for prefix: " + prefix,
          UserIdentityExtractionState.ERROR_INVALID_TOKEN, response.getState());
      Assert.assertNull(response.getIdentityPair());
      Assert.assertEquals(
          "Runtime access tokens cannot be used for user authentication",
          response.getErrorDescription());
    }
  }

  @Test
  public void testExtractExternalTokenSucceeds() throws Exception {
    String encodedToken = createSignedToken("external-user", UserIdentity.IdentifierType.EXTERNAL);
    UserIdentityExtractionResponse response = extractor.extract(createRequest("Bearer " + encodedToken));

    Assert.assertTrue(response.success());
    Assert.assertEquals(UserIdentityExtractionState.SUCCESS, response.getState());
    UserIdentityPair pair = response.getIdentityPair();
    Assert.assertNotNull(pair);
    Assert.assertEquals(encodedToken, pair.getUserCredential());
    Assert.assertEquals("external-user", pair.getUserIdentity().getUsername());
    Assert.assertEquals(UserIdentity.IdentifierType.EXTERNAL, pair.getUserIdentity().getIdentifierType());
  }

  @Test
  public void testExtractInternalTokenSucceeds() throws Exception {
    String encodedToken = createSignedToken("internal-user", UserIdentity.IdentifierType.INTERNAL);
    UserIdentityExtractionResponse response = extractor.extract(createRequest("CDAP-Internal " + encodedToken));

    Assert.assertTrue(response.success());
    Assert.assertEquals(UserIdentityExtractionState.SUCCESS, response.getState());
    UserIdentityPair pair = response.getIdentityPair();
    Assert.assertNotNull(pair);
    Assert.assertEquals(encodedToken, pair.getUserCredential());
    Assert.assertEquals("internal-user", pair.getUserIdentity().getUsername());
    Assert.assertEquals(UserIdentity.IdentifierType.INTERNAL, pair.getUserIdentity().getIdentifierType());
  }

  @Test
  public void testExtractMissingHeaderRejected() throws Exception {
    DefaultHttpRequest request = new DefaultHttpRequest(
        HttpVersion.HTTP_1_1, HttpMethod.GET, "http://www.example.com");
    UserIdentityExtractionResponse response = extractor.extract(request);

    Assert.assertFalse(response.success());
    Assert.assertEquals(UserIdentityExtractionState.ERROR_MISSING_CREDENTIAL, response.getState());
    Assert.assertNull(response.getIdentityPair());
  }

  private String createSignedToken(String username, UserIdentity.IdentifierType identifierType)
      throws IOException {
    long now = System.currentTimeMillis();
    UserIdentity identity = new UserIdentity(
        username, identifierType, Collections.emptyList(), now, now + TOKEN_TTL_MILLIS);
    AccessToken token = tokenManager.signIdentifier(identity);
    return Base64.getEncoder().encodeToString(accessTokenCodec.encode(token));
  }

  private static DefaultHttpRequest createRequest(String authHeader) {
    DefaultHttpHeaders headers = new DefaultHttpHeaders();
    headers.add(HttpHeaderNames.AUTHORIZATION, authHeader);
    return new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "http://www.example.com", headers);
  }
}
